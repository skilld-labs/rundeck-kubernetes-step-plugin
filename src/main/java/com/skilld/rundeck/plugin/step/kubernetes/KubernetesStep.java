/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * KubernetesStep.java
 *
 * User: Jean-Baptiste Guerraz <a href="mailto:jbguerraz@gmail.com">jbguerraz@gmail.com</a>
 * Created: 9/28/2016 1:37 PM
 *
 */
package com.skilld.rundeck.plugin.step.kubernetes;

import com.dtolabs.rundeck.core.common.Framework;
import com.dtolabs.rundeck.core.execution.workflow.steps.FailureReason;
import com.dtolabs.rundeck.core.execution.workflow.steps.StepException;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.core.plugins.configuration.Describable;
import com.dtolabs.rundeck.core.plugins.configuration.Description;
import com.dtolabs.rundeck.core.plugins.configuration.PropertyUtil;
import com.dtolabs.rundeck.plugins.PluginLogger;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.step.PluginStepContext;
import com.dtolabs.rundeck.plugins.step.StepPlugin;
import com.dtolabs.rundeck.plugins.util.DescriptionBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.skilld.kubernetes.JobConfiguration;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * KubernetesExecutor allow to run kubernetes jobs from rundeck
 * @author Jean-Baptiste Guerraz <a href="mailto:jbguerraz@gmail.com">jbguerraz@gmail.com</a>
 */
@Plugin(name = "kubernetes-step", service = "WorkflowStep")
@PluginDescription(title = "Kubernetes Jobs Execution", description = "Run a job through kubernetes.")
public class KubernetesStep implements StepPlugin, Describable {
	static Logger logger = Logger.getLogger(KubernetesStep.class);
	private Framework framework;

	public static final String STEP_NAME = "kubernetes-step";
	public static final String KUBE_MASTER = "kubeMaster";
	public static final String KUBE_TOKEN = "kubeToken";
	public static final String KUBE_SSL = "kubeSSL";
	public static final String IMAGE = "image";
	public static final String IMAGE_PULL_SECRETS = "imagePullSecrets";
	public static final String COMMAND = "command";
	public static final String ARGUMENTS = "arguments";
	public static final String NODE_SELECTOR = "nodeSelector";
	public static final String NAMESPACE = "namespace";
	public static final String ACTIVE_DEADLINE = "activeDeadlineSeconds";
	public static final String RESTART_POLICY = "restartPolicy";
	public static final String COMPLETIONS = "completions";
	public static final String PARALLELISM = "parallelism";
	public static final String PERSISTENT_VOLUME = "persistentVolume";
	public static final String SECRET = "secret";
	public static final String RESOURCE_REQUESTS = "resourceRequests";
	public static final String CLEAN_UP = "cleanUp";
	public static final String LABELS = "labels";

	private static final String LABELSEPARATOR = " ";
	private static final String LABELKVSEPARATOR = "=";

	private KubernetesClient client = null;
	private com.skilld.kubernetes.Job job = null;
	private	Watch jobWatch = null;
	private	Watch podWatch = null;

	public static enum Reason implements FailureReason {
		UnexepectedFailure,
		ExecutionTimeoutFailure,
		InterruptionFailure
	}

	public KubernetesStep(final Framework framework) {
		this.framework = framework;
	}

	static Description DESC = DescriptionBuilder.builder()
		.name(STEP_NAME)
		.title("Kubernetes")
		.description("Runs a Kubernetes job")
		.property(PropertyUtil.string(KUBE_MASTER, "Kubernetes URL", "The URL of the Kubernetes master, empty for local cluster", false, null))
		.property(PropertyUtil.string(KUBE_TOKEN, "Kubernetes Token", "The Service Token to use for Kubernetes, empty for local token", false, null))
		.property(PropertyUtil.bool(KUBE_SSL, "Kubernetes SSL Validate", "Validate the Kubernetes SSL server certificate", false, "true"))
		.property(PropertyUtil.string(IMAGE, "Image", "The container image to use", true, null))
		.property(PropertyUtil.string(IMAGE_PULL_SECRETS, "ImagePullSecrets", "The image pull secrets name", false, null))
		.property(PropertyUtil.string(COMMAND, "Command", "The command to run in the container", false, null))
		.property(PropertyUtil.string(ARGUMENTS, "Arguments", "The command arguments", false, null))
		.property(PropertyUtil.string(NODE_SELECTOR, "Node selector", "Kubernetes node label selector", false, null))
		.property(PropertyUtil.string(NAMESPACE, "Namespace", "Kubernetes namespace", true, "default"))
		.property(PropertyUtil.integer(ACTIVE_DEADLINE, "Active deadline", "The job deadline (in seconds)", false, null))
		.property(PropertyUtil.select(RESTART_POLICY, "Restart policy", "The restart policy to apply to the job", true, "Never", Arrays.asList("Never", "OnFailure")))
		.property(PropertyUtil.integer(COMPLETIONS, "Completions", "Number of pods to wait for success exit before considering the job complete", true, "1"))
		.property(PropertyUtil.integer(PARALLELISM, "Parallelism", "Number of pods running at any instant", true, "1"))
		.property(PropertyUtil.string(PERSISTENT_VOLUME, "Persistent Volume", "The name of the PVC to use in this job in format <name>;<mountpath>", false, null))
		.property(PropertyUtil.string(SECRET, "Secret", "The name of the kubernetes secret in format <name>;<mountpath>", false, null))
		.property(PropertyUtil.string(RESOURCE_REQUESTS, "Resource Requests", "Request resources in format cpu:4 memory:24Gi", false, null))
        .property(PropertyUtil.string(LABELS, "Labels", "The labels to set on the jobs. Labels are separated by '" + LABELSEPARATOR + "', keys and values by a '" + LABELKVSEPARATOR + "'. "
				+ "Example: 'foo" + LABELKVSEPARATOR + "bar" + LABELKVSEPARATOR + "a" + LABELSEPARATOR + "b'. See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set for "
				+ "information on key and value formatting.",false, ""))
		.property(PropertyUtil.bool(CLEAN_UP, "Cleanup", "Remove finished jobs from Kubernetes", true, "true"))
		.build();

	public Description getDescription() {
		return DESC;
	}

	public void executeStep(PluginStepContext context, Map<String,Object> configuration) throws StepException {
		PluginLogger pluginLogger = context.getLogger();
		ConfigBuilder clientConfigurationBuilder = new ConfigBuilder().withWatchReconnectInterval(30).withWatchReconnectLimit(0);

		if(null != configuration.get(KUBE_MASTER)) {
			clientConfigurationBuilder.withMasterUrl(configuration.get(KUBE_MASTER).toString());
		}
		if(null != configuration.get(KUBE_TOKEN)) {
			clientConfigurationBuilder.withOauthToken(configuration.get(KUBE_TOKEN).toString());
		}
		if(null != configuration.get(KUBE_SSL)) {
			clientConfigurationBuilder.withTrustCerts(!"true".equals(configuration.get(KUBE_SSL).toString()));
		}

		Config clientConfiguration = clientConfigurationBuilder.build();
		boolean cleanup = "true".equals(configuration.get(CLEAN_UP).toString());
		try {
			client = new DefaultKubernetesClient(clientConfiguration);
			String jobName = context.getDataContextObject().get("job").get("name").toString().toLowerCase() + "-" + context.getDataContextObject().get("job").get("execid");
			String namespace = configuration.get(NAMESPACE).toString();

			// add the job-name label in addition to the labels set in the job configuration
			final StringBuilder labelBuilder = new StringBuilder("job-name" + LABELKVSEPARATOR + jobName);
			if (configuration.containsKey(LABELS)) {
			    final String configuredLabels = configuration.get(LABELS).toString();
			    if (!configuredLabels.isEmpty()) {
					labelBuilder
							.append(LABELSEPARATOR)
							.append(configuration.get(LABELS).toString());
				}
			}
			// validate and get the labels
			final Map<String, String> labels = validateAndGetLabels(LABELKVSEPARATOR, LABELSEPARATOR, labelBuilder.toString());

			JobConfiguration jobConfiguration = new JobConfiguration();
			jobConfiguration.setName(jobName);
			jobConfiguration.setLabels(labels);
			jobConfiguration.setNamespace((String)configuration.get(NAMESPACE));
			jobConfiguration.setImage((String)configuration.get(IMAGE));
			jobConfiguration.setRestartPolicy((String)configuration.get(RESTART_POLICY));
			jobConfiguration.setCompletions(Integer.valueOf(configuration.get(COMPLETIONS).toString()));
			jobConfiguration.setParallelism(Integer.valueOf(configuration.get(PARALLELISM).toString()));
			if(null != configuration.get(IMAGE_PULL_SECRETS)){
				jobConfiguration.setImagePullSecrets(configuration.get(IMAGE_PULL_SECRETS).toString());
			}
			if(null != configuration.get(COMMAND)) {
				jobConfiguration.setCommand(configuration.get(COMMAND).toString(), context.getDataContextObject().get("option"));
			}
			if(null != configuration.get(ARGUMENTS)) {
				jobConfiguration.setArguments(configuration.get(ARGUMENTS).toString(), context.getDataContextObject().get("option"));
			}
			if(null != configuration.get(NODE_SELECTOR)) {
				jobConfiguration.setNodeSelector(configuration.get(NODE_SELECTOR).toString());
			}
			if(null != configuration.get(ACTIVE_DEADLINE)){
				jobConfiguration.setActiveDeadlineSeconds(Long.valueOf(configuration.get(ACTIVE_DEADLINE).toString()));
			}
			if(null != configuration.get(PERSISTENT_VOLUME)) {
				try {
					String persistentVolumeArray[] = configuration.get(PERSISTENT_VOLUME).toString().split("\\s*;\\s*");
					jobConfiguration.setPersistentVolume(persistentVolumeArray[0], persistentVolumeArray[1], context.getDataContextObject().get("option"));
				}
				catch (ArrayIndexOutOfBoundsException e) {
					logger.error("Invalid format for " + PERSISTENT_VOLUME, e);
				}
			}
			if(null != configuration.get(SECRET)) {
				try {
					String secretVolumeArray[] = configuration.get(SECRET).toString().split("\\s*;\\s*");
					jobConfiguration.setSecret(secretVolumeArray[0], secretVolumeArray[1], context.getDataContextObject().get("option"));
				}
				catch (ArrayIndexOutOfBoundsException e) {
					logger.error("Invalid format for " + SECRET, e);
				}
			}
			if(null != configuration.get(RESOURCE_REQUESTS) && !"".equals(configuration.get(RESOURCE_REQUESTS).toString())) {
				try {
					Map<String, Quantity> reqMap = new HashMap<>();
					for (String resourceRequest: configuration.get(RESOURCE_REQUESTS).toString().split(" ")) {
						String resourceRequestArray[] = resourceRequest.split(":");
						reqMap.put(resourceRequestArray[0], new Quantity(resourceRequestArray[1]));
					}
					jobConfiguration.setResourceRequests(reqMap);
				}
				catch (ArrayIndexOutOfBoundsException e) {
					logger.error("Invalid format for " + RESOURCE_REQUESTS, e);
				}
			}
			job = new com.skilld.kubernetes.Job(jobConfiguration);

			CountDownLatch jobCloseLatch = new CountDownLatch(1);
			Watcher<Job> jobWatcher = new Watcher<Job>() {
				@Override
				public void eventReceived(Action action, Job resource) {
					if(job.isComplete(resource)) {
						jobCloseLatch.countDown();
					}
				}
				@Override
				public void onClose(KubernetesClientException e) {
					if (null != e) {
						logger.error(e.getMessage());
					}
				}
			};
			Watcher<Pod> podWatcher = new Watcher<Pod>() {
				@Override
				public void eventReceived(Action action, Pod resource) {
					String name = resource.getMetadata().getName();
					Integer logLevel = null;
					if(resource.getStatus().getPhase().equals("Succeeded") && null == resource.getMetadata().getDeletionTimestamp()) {
						logLevel = 2;
					} else if (resource.getStatus().getPhase().equals("Failed")) {
						logLevel = 0;
					}
					if (null != logLevel) {
						pluginLogger.log(logLevel, name + " : " + client.pods().inNamespace(namespace).withName(name).getLog(true));
					}
				}
				@Override
				public void onClose(KubernetesClientException e) {
					if (null != e) {
						logger.error(e.getMessage());
					}
				}
			};

			jobWatch = client.extensions().jobs().inNamespace(namespace).withLabels(labels).watch(jobWatcher);
			podWatch = client.pods().inNamespace(namespace).withLabel("job-name", jobName).watch(podWatcher);
			client.extensions().jobs().inNamespace(namespace).withName(jobName).create(job.getJobResource());
			jobCloseLatch.await();
			Terminate(cleanup);

			if(job.hasFailed()){
				Reason reason = Reason.UnexepectedFailure;
				if(job.hasTimedout()){
					reason = Reason.ExecutionTimeoutFailure;
				}
				throw new StepException(job.getCompletionReason(), reason);
			}
		} catch (KubernetesClientException e) {
			logger.error(e.getMessage(), e);
			throw new StepException(e.getMessage(), Reason.UnexepectedFailure);
		} catch (InterruptedException e) {
			Terminate(cleanup);
			logger.error(e.getMessage(), e);
			throw new StepException(e.getMessage(), Reason.InterruptionFailure);
		} catch (StepException e) {
			logger.error(e.getMessage(), e);
			throw e;
		}
	}

	private void Terminate(boolean cleanup) {
		jobWatch.close();
		podWatch.close();
		if (cleanup) {
			job.delete(client);
		}
		client.close();
	}

	/**
	 * Validate a given label according to Kubernetes' label specification.
	 * The specification separates three label components:
	 * - the key, which in turn can consist of an optional prefix and a name
	 * - the value
	 *
	 * The prefix can be at most 253 characters and should be formatted as a DNS name.
	 * The name can be at most 63 characters and should start and end with an alphanumeric character and can contain
	 * alphanumeric or any of "-_.".
	 * The value validates the same as the name, but can be an empty string as well.
	 *
	 * For more information see https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
	 * @param separator The string (usually a single character) separating the key from the value. N.B.: There are no checks to make sure
	 * you don't separate on a character which will break validation, like "c" which will break because keys and values may contain "c" as well.
	 * @param keyValue The key-value pair to validate.
	 * @return True if {@code keyValue} validates, false if not.
	 */
	@VisibleForTesting
	protected static boolean validateLabel(String separator, String keyValue) {
		final String keyPrefixSeparator = "/";
	    final Predicate<String> isValidKeyName = Pattern.compile("^[a-zA-Z0-9]([-_.]?[a-zA-Z0-9]+)*$")
				.asPredicate()
				.and(prefix -> prefix.length() <= 63);
	    final Predicate<String> isValidKeyPrefix = Pattern.compile("^[a-zA-Z0-9]+(\\.[a-zA-Z0-9]+)*$")
				.asPredicate()
				.and(prefix -> prefix.length() <= 253);
	    final Predicate<String> isValidValue = isValidKeyName.or(""::equals);

	    // the separator may not be null or empty
		if (separator == null || separator.isEmpty()) {
			return false;
		}

		// the keyValue may not be null and should always contain at least a name and a separator, which in its shorted
		// form is "a="
		if (keyValue == null || keyValue.length() < 2) {
			return false;
		}

	    // a label cannot start with the separator, but it may end with it in case the value is empty
		if (keyValue.startsWith(separator)) {
			return false;
		}

	    // either keyvalue is of form "foo=bar", or "foo=" which will result in an array of 2 or 1 respectively
		// when split on "=" (assuming that's the separator used)
		final String[] kv = keyValue.split(separator);
		if (kv.length < 1 || kv.length > 2) {
			return false;
		}

		final String key = kv[0];
		// a value may be empty, so if we were given a label of form "foo=", set the value to an empty string
		final String val = kv.length == 2 ? kv[1] : "";

		// first make sure the key doesn't begin or end with a /
		if (key.startsWith(keyPrefixSeparator) || key.endsWith(keyPrefixSeparator)) {
			return false;
		}
		// split the key on /
		final String[] keySplitOnPrefix = key.split(keyPrefixSeparator);
		final boolean validKey;
		if (keySplitOnPrefix.length == 1) {
			// key doesn't contain a prefix
			validKey = isValidKeyName.test(keySplitOnPrefix[0]);
		} else if (keySplitOnPrefix.length == 2) {
			validKey = isValidKeyPrefix.test(keySplitOnPrefix[0]) &&
					isValidKeyName.test(keySplitOnPrefix[1]);
		} else {
			validKey = false;
		}

		return validKey && isValidValue.test(val);
	}

	/**
	 * Given a string, split it on {@code labelSeparator} and validate every substring according to {@link KubernetesStep#validateLabel(String, String)}.
	 * @param kvSeparator The string used to split a label to the key and the value.
	 * @param labelSeparator The string used to split occurrences of labels.
	 * @param labels The string that contains the label(s).
	 * @return A map containing the label-keys as keys and the label-values as values.
	 * @throws StepException Thrown when we can't validate one or more of the labels.
	 */
	@VisibleForTesting
	protected static Map<String, String> validateAndGetLabels(String kvSeparator, String labelSeparator, String labels) throws StepException {
		List<String> labelsUnvalidated = Arrays.asList(labels.split(labelSeparator));
		// make sure there is not a single label that doesn't validate
		if (!labelsUnvalidated.stream().allMatch(label -> validateLabel(kvSeparator, label))) {
			throw new StepException("Invalid label contained in \"" + labelsUnvalidated + "\"", Reason.UnexepectedFailure);
		}
		// now we know that all labels are valid, transform them to a map and return them
		return Arrays.asList(labels.split(labelSeparator))
				.stream()
				.map(keyVal -> keyVal.split(kvSeparator))
				.collect(Collectors.toMap(keyVal -> keyVal[0], keyVal -> keyVal.length == 2 ? keyVal[1] : ""));
	}
}
