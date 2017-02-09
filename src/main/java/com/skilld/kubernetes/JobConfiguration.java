/*
 * Licensed under the Apache License, Version 2.0 (the "License;
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
 * JobConfiguration.java
 *
 * User: Michaël Meunier <a href="mailto:michael.zxcv@gmail.com">michael.zxcv@gmail.com</a>
 * Created: 03/02/2017 2:33 PM
 *
 */
package com.skilld.kubernetes;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class JobConfiguration {

	private String name;
	private String image;
	private LocalObjectReference imagePullSecrets;
	private List<String> command;
	private List<String> arguments;
	private Long activeDeadlineSeconds;
	private String restartPolicy;
	private Integer completions;
	private String namespace;
	private Map<String, String> nodeSelector;
	private Integer parallelism;
	private Map<String, String> labels;

	/* Getters */
	public String getName() {
		return name;
	}

	public String getImage() {
		return image;
	}

	public LocalObjectReference getImagePullSecrets() {
		return imagePullSecrets;
	}

	public List<String> getCommand() {
		return command;
	}

	public List<String> getArguments() {
		return arguments;
	}

	public Map<String, String> getNodeSelector() {
		return nodeSelector;
	}

	public String getNamespace() {
		return namespace;
	}

	public Long getActiveDeadlineSeconds() {
		return activeDeadlineSeconds;
	}

	public String getRestartPolicy() {
		return restartPolicy;
	}

	public Integer getCompletions() {
		return completions;
	}

	public Integer getParallelism() {
		return parallelism;
	}

	public Map<String, String> getLabels(){
		return labels;
	}

	/* Setters */
	public void setName(String _name) {
		name = _name;
	}

	public void setImage(String _image) {
		image = _image;
	}

	public void setImagePullSecrets(String _imagePullSecrets) {
		imagePullSecrets = new LocalObjectReference(_imagePullSecrets);
	}

	public void setCommand(String _command, Map<String, String> _options) {
		command = buildInput(_command, _options);
	}

	public void setArguments(String _arguments, Map<String, String> _options) {
		arguments = buildInput(_arguments, _options);
	}

	public void setNodeSelector(String _nodeSelector) {
		nodeSelector = (HashMap<String, String>) Arrays.asList(_nodeSelector.split(",")).stream().map(s -> s.split("=")).collect(Collectors.toMap(e -> e[0], e -> e[1]));
	}

	public void setNamespace(String _namespace) {
		namespace = _namespace;
	}

	public void setActiveDeadlineSeconds(Long _activeDeadlineSeconds) {
		activeDeadlineSeconds = _activeDeadlineSeconds;
	}

	public void setRestartPolicy(String _restartPolicy) {
		restartPolicy = _restartPolicy;
	}

	public void setCompletions(Integer _completions) {
		completions = _completions;
	}

	public void setParallelism(Integer _parallelism) {
		parallelism = _parallelism;
	}

	public void setLabels(Map<String, String> _labels) {
		labels = _labels;
	}

	private List<String> buildInput(String _input, Map<String,String> _options){
		for (Map.Entry<String, String> option : _options.entrySet()){
			_input = _input.replace("${" + option.getKey() + "}", option.getValue());
		}
		List<String> input = new ArrayList<String>();
		Matcher inputParts = Pattern.compile("(\"(?:.(?!(?<!\\\\)\"))*.?\"|'(?:.(?!(?<!\\\\)'))*.?'|\\S+)").matcher(_input);
		while(inputParts.find()) {
			input.add(inputParts.group(1));
		}
		return input;
	}
}
