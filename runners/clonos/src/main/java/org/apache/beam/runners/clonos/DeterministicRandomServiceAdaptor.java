package org.apache.beam.runners.clonos;

import org.apache.beam.sdk.transforms.workaround.RandomService;

import java.io.Serializable;

public class DeterministicRandomServiceAdaptor implements RandomService, Serializable {

    private final org.apache.flink.api.common.services.RandomService randomService;

    public DeterministicRandomServiceAdaptor(org.apache.flink.api.common.services.RandomService randomService){
        this.randomService = randomService;
    }


    @Override
    public int nextInt() {
        return randomService.nextInt();
    }

    @Override
    public int nextInt(int max) {
        return randomService.nextInt(max);
    }
}
