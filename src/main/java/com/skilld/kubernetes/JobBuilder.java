/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * JobBuilder.java
 *
 * User: Michael Meunier <a href="mailto:michael.zxcv@gmail.com">michael.zxcv@gmail.com</a>
 * Created: 3/03/2017 1:37 PM
 *
 */

package com.skilld.kubernetes;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;

import java.util.List;
import java.util.Map;

public class JobBuilder {
	public static Job build(JobConfiguration configuration) {
		io.fabric8.kubernetes.api.model.JobBuilder jobBuilder = new io.fabric8.kubernetes.api.model.JobBuilder()
			.withNewMetadata()
				.withName(configuration.getName())
				.withNamespace(configuration.getNamespace())
			.endMetadata()
			.withNewSpec()
				.withParallelism(configuration.getParallelism())
				.withCompletions(configuration.getCompletions())
				.withNewTemplate()
                    .withNewMetadata()
                        .withLabels(configuration.getLabels())
                    .endMetadata()
					.withNewSpec()
						.withRestartPolicy(configuration.getRestartPolicy())
						.addNewContainer()
							.withName(configuration.getName())
							.withImage(configuration.getImage())
							.withImagePullPolicy(configuration.getImagePullPolicy())
						.endContainer()
					.endSpec()
				.endTemplate()
			.endSpec();
		Long activeDeadlineSeconds = null;
		if(null != configuration.getActiveDeadlineSeconds()){
			activeDeadlineSeconds = configuration.getActiveDeadlineSeconds();
			jobBuilder
				.editSpec()
					.withActiveDeadlineSeconds(activeDeadlineSeconds)
				.endSpec();
		}
		if(null != configuration.getImagePullSecrets()){
			jobBuilder
				.editSpec()
					.editTemplate()
						.editSpec()
							.withImagePullSecrets(configuration.getImagePullSecrets())
						.endSpec()
					.endTemplate()
				.endSpec();
		}
		if(null != configuration.getNodeSelector()) {
			jobBuilder
				.editSpec()
					.editTemplate()
						.editSpec()
							.withNodeSelector(configuration.getNodeSelector())
						.endSpec()
					.endTemplate()
				.endSpec();
		}

		Container container = jobBuilder.buildSpec().getTemplate().getSpec().getContainers().get(0);
		List<VolumeMount> volumeMountList = container.getVolumeMounts();
		if(null != configuration.getCommand()) {
			container.setCommand(configuration.getCommand());
		}
		if(null != configuration.getArguments()) {
			container.setArgs(configuration.getArguments());
		}
		Map<String, String> persistentVolumes = configuration.getPersistentVolumes();
		if(null != persistentVolumes && persistentVolumes.size() > 0) {
			for(Map.Entry<String, String> entry : persistentVolumes.entrySet()) {
				volumeMountList.add(
					new VolumeMount(
						entry.getValue(),
						entry.getKey(),
						Boolean.FALSE,
						null
					)
				);

				jobBuilder
					.editSpec()
						.editTemplate()
							.editSpec()
								.addNewVolume()
									.withName(entry.getKey())
									.withNewPersistentVolumeClaim(entry.getKey(), false)
								.endVolume()
							.endSpec()
						.endTemplate()
					.endSpec();
			}
		}
		Map<String, String> secrets = configuration.getSecrets();
		if(null != secrets &&secrets.size() > 0) {
			for(Map.Entry<String, String> entry : secrets.entrySet()) {
				volumeMountList.add(
					new VolumeMount(
						entry.getValue(),
						entry.getKey(),
						Boolean.TRUE,
						null
					)
				);

				jobBuilder
					.editSpec()
						.editTemplate()
							.editSpec()
								.addNewVolume()
									.withName(entry.getKey())
									.withNewSecret()
										.withSecretName(entry.getKey())
									.endSecret()
								.endVolume()
							.endSpec()
						.endTemplate()
					.endSpec();
			}
		}
		container.setVolumeMounts(volumeMountList);

		if(null != configuration.getResourceRequests()) {
			container.setResources(
				new ResourceRequirementsBuilder()
					.withRequests(configuration.getResourceRequests())
					.build()
			);
		}

		jobBuilder
			.editSpec()
				.editTemplate()
					.editSpec()
						.withContainers(container)
					.endSpec()
				.endTemplate()
			.endSpec();
		return jobBuilder.build();
	}
}
