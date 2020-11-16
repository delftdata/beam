package org.apache.beam.sdk.nexmark.model.workaround;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.transforms.Combine;

import java.util.Iterator;

public class LatTSPropagatingMaxLong extends Combine.CombineFn<LatTSWrapped<Long>, LatTSWrapped<Long>, LatTSWrapped<Long>> {

    @Override
    public LatTSWrapped<Long> createAccumulator() {
        return LatTSWrapped.of(Long.MIN_VALUE, Long.MIN_VALUE);
    }

    @Override
    public LatTSWrapped<Long> addInput(LatTSWrapped<Long> mutableAccumulator, LatTSWrapped<Long> input) {
        return LatTSWrapped.of(
                mutableAccumulator.getValue() > input.getValue() ? mutableAccumulator.getValue() : input.getValue(),
                mutableAccumulator.getLatTS(), input.getLatTS());
    }

    @Override
    public LatTSWrapped<Long> mergeAccumulators(Iterable<LatTSWrapped<Long>> accumulators) {
        long maxTS = Long.MIN_VALUE;
        long maxVal = Long.MIN_VALUE;

        for (LatTSWrapped<Long> accum : accumulators) {
            maxTS = maxTS > accum.getLatTS() ? maxTS : accum.getLatTS();
            maxVal = maxVal > accum.getValue() ? maxVal : accum.getValue();
        }

        return LatTSWrapped.of(maxVal, maxTS);
    }

    @Override
    public LatTSWrapped<Long> extractOutput(LatTSWrapped<Long> accumulator) {
        return accumulator;
    }

    @Override
    public Coder<LatTSWrapped<Long>> getAccumulatorCoder(CoderRegistry registry, Coder<LatTSWrapped<Long>> inputCoder) {
        return inputCoder;
    }

    @Override
    public Coder<LatTSWrapped<Long>> getDefaultOutputCoder(CoderRegistry registry, Coder<LatTSWrapped<Long>> inputCoder) {
        return inputCoder;
    }

}


