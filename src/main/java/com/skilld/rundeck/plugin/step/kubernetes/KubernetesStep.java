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

import com.skilld.kubernetes.JobConfiguration;

import com.dtolabs.rundeck.core.common.Framework;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.core.plugins.configuration.*;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.util.DescriptionBuilder;
import com.dtolabs.rundeck.plugins.util.PropertyBuilder;
import com.dtolabs.rundeck.plugins.step.StepPlugin;
import com.dtolabs.rundeck.core.execution.workflow.steps.FailureReason;
import com.dtolabs.rundeck.core.execution.workflow.steps.StepException;
import com.dtolabs.rundeck.plugins.step.PluginStepContext;
import com.dtolabs.rundeck.plugins.PluginLogger;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import static io.fabric8.kubernetes.client.Watcher.Action.ERROR;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.Pod;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.log4j.Logger;

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
		.build();

	public Description getDescription() {
		return DESC;
	}

	public void executeStep(PluginStepContext context, Map<String,Object> configuration) throws StepException {
		PluginLogger pluginLogger = context.getLogger();
		Config clientConfiguration = new ConfigBuilder().withWatchReconnectLimit(2).build();

		try {
			client = new DefaultKubernetesClient(clientConfiguration);
			String jobName = context.getDataContext().get("job").get("name").toString().toLowerCase() + "-" + context.getDataContext().get("job").get("execid");
			String namespace = configuration.get("namespace").toString();
			Map<String, String> labels = new HashMap<String, String>();
			labels.put("job-name", jobName);

			JobConfiguration jobConfiguration = new JobConfiguration();
			jobConfiguration.setName(jobName);
			jobConfiguration.setLabels(labels);
			jobConfiguration.setNamespace((String)configuration.get("namespace"));
			jobConfiguration.setImage((String)configuration.get("image"));
			jobConfiguration.setRestartPolicy((String)configuration.get("restartPolicy"));
			jobConfiguration.setCompletions(Integer.valueOf(configuration.get("completions").toString()));
			jobConfiguration.setParallelism(Integer.valueOf(configuration.get("parallelism").toString()));
			if(null != configuration.get("imagePullSecrets")){
				jobConfiguration.setImagePullSecrets(configuration.get("imagePullSecrets").toString());
			}
			if(null != configuration.get("command")) {
				jobConfiguration.setCommand(configuration.get("command").toString(), context.getDataContext().get("option"));
			}
			if(null != configuration.get("arguments")) {
				jobConfiguration.setArguments(configuration.get("arguments").toString(), context.getDataContext().get("option"));
			}
			if(null != configuration.get("nodeSelector")) {
				jobConfiguration.setNodeSelector(configuration.get("nodeSelector").toString());
			}
			if(null != configuration.get("activeDeadlineSeconds")){
				jobConfiguration.setActiveDeadlineSeconds(Long.valueOf(configuration.get("activeDeadlineSeconds").toString()));
			}
			job = new com.skilld.kubernetes.Job(jobConfiguration);

			CountDownLatch jobCloseLatch = new CountDownLatch(1);
			Watcher jobWatcher = new Watcher<Job>() {
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
			Watcher podWatcher = new Watcher<Pod>() {
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
			Terminate();

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
			Terminate();
			logger.error(e.getMessage(), e);
			throw new StepException(e.getMessage(), Reason.InterruptionFailure);
		} catch (StepException e) {
			logger.error(e.getMessage(), e);
			throw e;
		}
	}

	private void Terminate() {
		jobWatch.close();
		podWatch.close();
		job.delete(client);
		client.close();
	}
}
