package org.apache.beam.sdk.transforms.workaround;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

public class ThreadLocalRandomServiceAdaptor implements RandomService, Serializable {

    @Override
    public int nextInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    @Override
    public int nextInt(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }
}
