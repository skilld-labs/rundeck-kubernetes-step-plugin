package com.skilld.rundeck.plugin.step.kubernetes;

import com.dtolabs.rundeck.core.execution.workflow.steps.StepException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class KubernetesStepTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private static final String KVSEPARATOR = "=";
    private static final String SEPARATOR = " ";

    private static final Function<String, Boolean> testwithSeparator =
            label -> KubernetesStep.validateLabel(KVSEPARATOR, label);

    // Validate single labels
    @Test
    public void validateLabel() {
        // make sure both separator and keyValue cannot be null or empty
        assertFalse(KubernetesStep.validateLabel(null, "foo=bar"));
        assertFalse(KubernetesStep.validateLabel("", "foo=bar"));
        assertFalse(KubernetesStep.validateLabel(KVSEPARATOR, null));
        assertFalse(KubernetesStep.validateLabel(KVSEPARATOR, ""));

        // test some valid cases
        assertTrue(testwithSeparator.apply("kubernetes.io/foo=bar"));
        assertTrue(testwithSeparator.apply("k.uber.netes.io/F_o-0=b.a-r"));
        assertTrue(testwithSeparator.apply("foo=bar"));
        assertTrue(testwithSeparator.apply("a="));

        // test some invalid cases
        assertFalse(testwithSeparator.apply("=a"));
        assertFalse(testwithSeparator.apply("/foo=bar"));
        assertFalse(testwithSeparator.apply("foo/=bar"));
        assertFalse(testwithSeparator.apply("kubernetes-io/foo=bar"));
        assertFalse(testwithSeparator.apply("kubernetes io/foo=bar"));
        assertFalse(testwithSeparator.apply("kubernetes.io/foo=b ar"));
        assertFalse(testwithSeparator.apply("_foo=bar"));
        assertFalse(testwithSeparator.apply("-foo=bar"));
        assertFalse(testwithSeparator.apply(".foo=bar"));
        assertFalse(testwithSeparator.apply("foo=bar_"));
        assertFalse(testwithSeparator.apply("foo=bar-"));
        assertFalse(testwithSeparator.apply("foo=bar."));

        // components of labels are only allowed a certain length

        // create a string of 253 a's, which is the maximum length prefix allowed in the key
        final String maxLengthPrefix = IntStream.range(0, 253)
                .mapToObj((i) -> "a")
                .collect(Collectors.joining());
        // test if it validates
        assertTrue(testwithSeparator.apply(maxLengthPrefix + "/foo=bar"));
        // make the prefix one character longer, it shouldn't validate
        assertFalse(testwithSeparator.apply(maxLengthPrefix + "a/foo=bar"));

        // the name part of the key can be at most 63 characters long
        final String maxLengthName = IntStream.range(0, 63)
                .mapToObj((i) -> "b")
                .collect(Collectors.joining());
        // test if it validates
        assertTrue(testwithSeparator.apply(maxLengthPrefix + "/" + maxLengthName + "=bar"));
        // make the name of the key one character too long
        assertFalse(testwithSeparator.apply(maxLengthPrefix + "/b" + maxLengthName + "=bar"));

        // the value can be at most 63 characters long, like the name of the key
        final String maxLengthVal = IntStream.range(0, 63)
                .mapToObj((i) -> "c")
                .collect(Collectors.joining());
        // test if it validates
        assertTrue(testwithSeparator.apply(
                maxLengthPrefix + "/" + maxLengthName + KVSEPARATOR + maxLengthVal));
        // make the val one character too long
        assertFalse(testwithSeparator.apply(
                maxLengthPrefix + "/" + maxLengthName + "=c" + maxLengthVal));
    }

    // validate some strings containing multiple labels
    @Test
    public void validateAndGetLabels() throws Exception {
        final String valid01 = "foo" + KVSEPARATOR + "bar" + SEPARATOR + "blaat" + KVSEPARATOR + "koe";
        final Map<String, String> result01 = KubernetesStep.validateAndGetLabels(KVSEPARATOR, SEPARATOR, valid01);

        assertEquals(2, result01.size());
        assertTrue(result01.containsKey("foo"));
        assertEquals("bar", result01.get("foo"));

        assertTrue(result01.containsKey("blaat"));
        assertEquals("koe", result01.get("blaat"));

        final String invalid01 = "f!oo=bar blaat=koe";
        exception.expect(StepException.class);
        KubernetesStep.validateAndGetLabels(KVSEPARATOR, SEPARATOR, invalid01);
    }
}