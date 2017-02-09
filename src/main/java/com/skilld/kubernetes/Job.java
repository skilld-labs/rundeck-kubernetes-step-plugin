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
 * Job.java
 *
 * User: Jean-Baptiste Guerraz <a href="mailto:jbguerraz@gmail.com">jbguerraz@gmail.com</a>
 * Created: 9/28/2016 1:37 PM
 *
 */
package com.skilld.kubernetes;

import com.skilld.kubernetes.JobConfiguration;
import com.skilld.kubernetes.JobBuilder;

import io.fabric8.kubernetes.api.model.JobStatus;
import io.fabric8.kubernetes.api.model.JobCondition;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;

public class Job {
	private io.fabric8.kubernetes.api.model.Job job = null;
	private JobCondition jobCondition = null;

	public Job (JobConfiguration jobConfiguration){
		job = JobBuilder.build(jobConfiguration);
	}

	public io.fabric8.kubernetes.api.model.Job getJobResource() {
		return job;
	}

	public Boolean isComplete(io.fabric8.kubernetes.api.model.Job _job) {
		String type = null;
		for (JobCondition condition : _job.getStatus().getConditions()) {
			type = condition.getType();
			if(type.equals("Complete") || type.equals("Failed")){
				jobCondition = condition;
				break;
			}
		}
		return null != jobCondition;
	}
	public Boolean hasFailed() {
		return null != jobCondition && jobCondition.getType().equals("Failed");
	}
	public Boolean hasTimedout() {
		return null != jobCondition && jobCondition.getReason().contains("Deadline");
	}
	public String getCompletionReason() {
		return (null != jobCondition) ? jobCondition.getReason() : null;
	}
	public void delete(io.fabric8.kubernetes.client.KubernetesClient client) {
		String jobName = job.getMetadata().getName();
		String namespace = job.getMetadata().getNamespace();
		client.extensions().jobs().inNamespace(namespace).withName(jobName).delete();
		PodList podList = client.pods().inNamespace(namespace).withLabel("job-name", jobName).list();
		for (Pod pod : podList.getItems()) {
			client.pods().inNamespace(namespace).withName(pod.getMetadata().getName()).delete();
		}
	}
}
