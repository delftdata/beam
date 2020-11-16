/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.nexmark.model.workaround;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

public class LatTSWrapped<V> extends Number implements Serializable, KnownSize {


    private final V value;
    private final long timestamp;

    protected LatTSWrapped(V value, long timestamp) {
        checkNotNull(timestamp, "timestamp must be non-null");

        this.value = value;
        this.timestamp = timestamp;
    }

    public static <V> LatTSWrapped<V> of(V value, long timestamp) {
        return new LatTSWrapped<>(value, timestamp);
    }

    public static <V> LatTSWrapped<V> of(V value, long timestamp1, long timestamp2) {
        return new LatTSWrapped<>(value, Math.max(timestamp1, timestamp2));
    }

    public static <V> LatTSWrapped<V> of(V value) {
        return new LatTSWrapped<>(value, System.currentTimeMillis());
    }

    public V getValue() {
        return value;
    }

    public long getLatTS() {
        return timestamp;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LatTSWrapped)) {
            return false;
        }
        LatTSWrapped<?> that = (LatTSWrapped<?>) other;
        return Objects.equals(value, that.value) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return  timestamp + "," + value;
    }

    @Override
    public long sizeInBytes() {
        if(value instanceof KnownSize)
            return ((KnownSize) value).sizeInBytes() + 8;
        if(value instanceof Boolean)
            return 1 + 8;
        if(value instanceof Long)
            return 8 + 8;
        return 8;
    }

    @Override
    public int intValue() {
        return ((Number)value).intValue();
    }

    @Override
    public long longValue() {
        return ((Number)value).longValue();
    }

    @Override
    public float floatValue() {
        return ((Number)value).floatValue();
    }

    @Override
    public double doubleValue() {
        return ((Number)value).doubleValue();
    }


    /////////////////////////////////////////////////////////////////////////////

    public static class LatTSWrappedValueCoder<T> extends StructuredCoder<LatTSWrapped<T>> {

        private static final Coder<Long> LONG_CODER = VarLongCoder.of();
        private final Coder<T> valueCoder;

        public static <T> LatTSWrapped.LatTSWrappedValueCoder<T> of(Coder<T> valueCoder) {
            return new LatTSWrapped.LatTSWrappedValueCoder<>(valueCoder);
        }

        @Override
        public Object structuralValue(LatTSWrapped<T> value) {
            Object structuralValue = valueCoder.structuralValue(value.getValue());
            return LatTSWrapped.of( structuralValue, value.getLatTS());
        }

        @SuppressWarnings("unchecked")
        LatTSWrappedValueCoder(Coder<T> valueCoder) {
            this.valueCoder = checkNotNull(valueCoder);
        }

        @Override
        public void encode(LatTSWrapped<T> windowedElem, OutputStream outStream)
                throws IOException {
            valueCoder.encode(windowedElem.getValue(), outStream);
            LONG_CODER.encode(windowedElem.getLatTS(), outStream);
        }

        @Override
        public LatTSWrapped<T> decode(InputStream inStream) throws IOException {
            T value = valueCoder.decode(inStream);
            long timestamp = LONG_CODER.decode(inStream);
            return LatTSWrapped.of(value, timestamp);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            verifyDeterministic(
                    this, "TimestampedValueCoder requires a deterministic valueCoder", valueCoder);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Arrays.<Coder<?>>asList(valueCoder);
        }

        public Coder<T> getValueCoder() {
            return valueCoder;
        }

        @Override
        public TypeDescriptor<LatTSWrapped<T>> getEncodedTypeDescriptor() {
            return new TypeDescriptor<LatTSWrapped<T>>() {}.where(
                    new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
        }

        @Override
        public List<? extends Coder<?>> getComponents() {
            return Collections.singletonList(valueCoder);
        }
    }

    /////////////////////////////////////////////////////////////////////////////


}
