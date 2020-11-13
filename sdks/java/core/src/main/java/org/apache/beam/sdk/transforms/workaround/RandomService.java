package org.apache.beam.sdk.transforms.workaround;

public interface RandomService {

    int nextInt();
    int nextInt(int max);

}
