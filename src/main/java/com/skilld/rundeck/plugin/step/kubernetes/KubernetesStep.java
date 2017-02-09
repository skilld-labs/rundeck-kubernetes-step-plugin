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
import io.fabric8.kubernetes.api.model.extensions.Job;
import io.fabric8.kubernetes.api.model.extensions.JobBuilder;
import io.fabric8.kubernetes.api.model.extensions.JobStatus;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Container;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


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

	public static enum Reason implements FailureReason {
		UnexepectedFailure
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
		try (KubernetesClient client = new DefaultKubernetesClient(clientConfiguration)) {
			String jobName = context.getDataContext().get("job").get("name").toString().toLowerCase() + "-" + context.getDataContext().get("job").get("execid");
			String namespace = configuration.get("namespace").toString();
			Map<String, String> labels = new HashMap<String, String>();
			labels.put("job-name", jobName);

			JobBuilder jobBuilder = new JobBuilder()
				.withNewMetadata()
					.withName(jobName)
					.withNamespace(namespace)
				.endMetadata()
				.withNewSpec()
					.withNewSelector()
						.withMatchLabels(labels)
					.endSelector()
					.withParallelism(Integer.valueOf(configuration.get("parallelism").toString()))
					.withCompletions(Integer.valueOf(configuration.get("completions").toString()))
					.withNewTemplate()
						.withNewMetadata()
							.withLabels(labels)
						.endMetadata()
						.withNewSpec()
							.withRestartPolicy(configuration.get("restartPolicy").toString())
							.addNewContainer()
								.withName(jobName)
								.withImage(configuration.get("image").toString())
							.endContainer()
						.endSpec()
					.endTemplate()
				.endSpec();

			Long activeDeadlineSeconds = null;
			if(null != configuration.get("activeDeadlineSeconds")){
				activeDeadlineSeconds = Long.valueOf(configuration.get("activeDeadlineSeconds").toString());
				jobBuilder
					.editSpec()
						.withActiveDeadlineSeconds(activeDeadlineSeconds)
					.endSpec();
			}
			if(null != configuration.get("imagePullSecrets")){
				jobBuilder
					.editSpec()
						.editTemplate()
							.editSpec()
								.withImagePullSecrets(new LocalObjectReference(configuration.get("imagePullSecrets").toString()))
							.endSpec()
						.endTemplate()
					.endSpec();
			}
			if(null != configuration.get("nodeSelector")) {
				jobBuilder
					.editSpec()
						.editTemplate()
							.editSpec()
								.withNodeSelector((HashMap<String, String>) Arrays.asList(configuration.get("nodeSelector").toString().split(",")).stream().map(s -> s.split("=")).collect(Collectors.toMap(e -> e[0], e -> e[1])))
							.endSpec()
						.endTemplate()
					.endSpec();
			}
			Container container = jobBuilder.getSpec().getTemplate().getSpec().getContainers().get(0);
			if(null != configuration.get("command")) {
				String _command = configuration.get("command").toString();
				for (Map.Entry<String, String> option : context.getDataContext().get("option").entrySet()) {
					_command = _command.replace("${" + option.getKey() + "}", option.getValue());
				}
				List<String> command = new ArrayList<String>();
				Matcher commandParts = Pattern.compile("(\"(?:.(?!(?<!\\\\)\"))*.?\"|'(?:.(?!(?<!\\\\)'))*.?'|\\S+)").matcher(_command);
				while (commandParts.find()) {
					command.add(commandParts.group(1));//.replace("\"", ""));
				}
				container.setCommand(command);
			}
			if(null != configuration.get("arguments")) {
				String _arguments = configuration.get("arguments").toString();
				for (Map.Entry<String, String> option : context.getDataContext().get("option").entrySet()) {
					_arguments = _arguments.replace("${" + option.getKey() + "}", option.getValue());
				}
				List<String> arguments = new ArrayList<String>();
				Matcher argumentsParts = Pattern.compile("(\"(?:.(?!(?<!\\\\)\"))*.?\"|'(?:.(?!(?<!\\\\)'))*.?'|\\S+)").matcher(_arguments);
				while (argumentsParts.find()) {
					arguments.add(argumentsParts.group(1));//.replace("\"", ""));
				}
				container.setArgs(arguments);
			}
			jobBuilder
				.editSpec()
					.editTemplate()
						.editSpec()
							.withContainers(container)
						.endSpec()
					.endTemplate()
				.endSpec();

			CountDownLatch jobCloseLatch = new CountDownLatch(1);
			Watcher jobWatcher = new Watcher<Job>() {
				@Override
				public void eventReceived(Action action, Job resource) {
					if(resource.getStatus().getCompletionTime() != null)
					{
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
					String deletionTimeStamp = resource.getMetadata().getDeletionTimestamp();
					String phase = resource.getStatus().getPhase();
					if(phase.equals("Succeeded") && null ==	deletionTimeStamp) {
						pluginLogger.log(2, name + " : " + client.pods().inNamespace(namespace).withName(name).getLog(true));
					}
					if(phase.equals("Failed")) {
						pluginLogger.log(0, name + " : " + client.pods().inNamespace(namespace).withName(name).getLog(true));
					}
				}
				@Override
				public void onClose(KubernetesClientException e) {
					if (null != e) {
						logger.error(e.getMessage());
					}
				}
			};
			try(Watch jobWatch = client.extensions().jobs().inNamespace(namespace).withLabels(labels).watch(jobWatcher)) {
				try(Watch podWatch = client.pods().inNamespace(namespace).withLabel("job-name", jobName).watch(podWatcher)) {
					client.extensions().jobs().inNamespace(namespace).withName(jobName).create(jobBuilder.build());
					if(null != activeDeadlineSeconds) {
						jobCloseLatch.await(activeDeadlineSeconds, TimeUnit.SECONDS);
					}
					else {
						jobCloseLatch.await();
					}
					jobWatch.close();
					podWatch.close();
					client.extensions().jobs().inNamespace(namespace).withName(jobName).delete();
					PodList podList = client.pods().inNamespace(namespace).withLabel("job-name", jobName).list();
					String name = null;
					for (Pod pod : podList.getItems()) {
						name = pod.getMetadata().getName();
						if(pod.getStatus().getPhase().equals("Pending")) {
							pluginLogger.log(0, name + " : Timeout");
						}
						client.pods().inNamespace(namespace).withName(name).delete();
					}
					client.close();
				} catch (KubernetesClientException | InterruptedException e) {
					logger.error(e.getMessage(), e);
					throw new StepException(e.getMessage(), Reason.UnexepectedFailure);
				}
			} catch (KubernetesClientException | StepException e) {
				logger.error(e.getMessage(), e);
				throw e;
			}
		} catch (KubernetesClientException | StepException e) {
			logger.error(e.getMessage(), e);
			throw new StepException(e.getMessage(), Reason.UnexepectedFailure);
		}
	}
}
