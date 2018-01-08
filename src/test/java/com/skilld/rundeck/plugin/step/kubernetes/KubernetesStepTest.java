package com.skilld.rundeck.plugin.step.kubernetes;

import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class KubernetesStepTest {

    private static final String SEPARATOR = "=";
    @Test
    public void validateLabel() {
        // make sure both separator and keyValue cannot be null or empty
        assertFalse(KubernetesStep.validateLabel(null, "foo=bar"));
        assertFalse(KubernetesStep.validateLabel("", "foo=bar"));
        assertFalse(KubernetesStep.validateLabel(SEPARATOR, null));
        assertFalse(KubernetesStep.validateLabel(SEPARATOR, ""));

        // test some valid cases
        Arrays.asList(
                "kubernetes.io/foo=bar",
                "k.uber.netes.io/F_o-0=b.a-r",
                "foo=bar",
                "a="
        ).stream()
                .forEach(label -> assertTrue(KubernetesStep.validateLabel(SEPARATOR, label)));

        // test some invalid cases
        Arrays.asList(
                "=a",
                "/foo=bar",
                "foo/=bar",
                "kubernetes-io/foo=bar",
                "kubernetes io/foo=bar",
                "kubernetes.io/foo=b ar",
                "_foo=bar",
                "-foo=bar",
                ".foo=bar",
                "foo=bar_",
                "foo=bar-",
                "foo=bar."
        ).stream()
                .forEach(label -> assertFalse(KubernetesStep.validateLabel(SEPARATOR, label)));

        // components of labels are only allowed a certain length

        // create a string of 253 a's, which is the maximum length prefix allowed in the key
        final String maxLengthPrefix = IntStream.range(0, 253)
                .mapToObj((i) -> "a")
                .collect(Collectors.joining());
        // test if it validates
        assertTrue(KubernetesStep.validateLabel(SEPARATOR, maxLengthPrefix + "/foo=bar"));
        // make the prefix one character longer, it shouldn't validate
        assertFalse(KubernetesStep.validateLabel(SEPARATOR, maxLengthPrefix + "a/foo=bar"));

        // the name part of the key can be at most 63 characters long
        final String maxLengthName = IntStream.range(0, 63)
                .mapToObj((i) -> "b")
                .collect(Collectors.joining());
        // test if it validates
        assertTrue(KubernetesStep.validateLabel(SEPARATOR, maxLengthPrefix + "/" + maxLengthName + "=bar"));
        // make the name of the key one character too long
        assertFalse(KubernetesStep.validateLabel(SEPARATOR, maxLengthPrefix + "/b" + maxLengthName + "=bar"));

        // the value can be at most 63 characters long, like the name of the key
        final String maxLengthVal = IntStream.range(0, 63)
                .mapToObj((i) -> "c")
                .collect(Collectors.joining());
        // test if it validates
        assertTrue(KubernetesStep.validateLabel(SEPARATOR,
                maxLengthPrefix + "/" + maxLengthName + SEPARATOR + maxLengthVal));
        // make the val one character too long
        assertFalse(KubernetesStep.validateLabel(SEPARATOR,
                maxLengthPrefix + "/" + maxLengthName + "=c" + maxLengthVal));
    }
}