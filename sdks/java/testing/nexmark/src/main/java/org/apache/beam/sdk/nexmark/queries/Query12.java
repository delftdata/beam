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
package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.BidsPerSession;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.workaround.LatTSWrapped;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Query "12", 'Processing time windows' (Not in original suite.)
 *
 * <p>Group bids by the same user into processing time windows of windowSize. Emit the count of bids
 * per window.
 */
public class Query12 extends NexmarkQueryTransform<BidsPerSession> {
  private final NexmarkConfiguration configuration;

  public Query12(NexmarkConfiguration configuration) {
    super("Query12");
    this.configuration = configuration;
  }

  @Override
  public PCollection<LatTSWrapped<BidsPerSession>> expand(PCollection<LatTSWrapped<Event>> events) {
    return events
        .apply(NexmarkQueryUtil.JUST_BIDS)
        .apply(
            ParDo.of(
                new DoFn<LatTSWrapped<Bid>, LatTSWrapped<Long>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(LatTSWrapped.of(c.element().getValue().bidder, c.element().getLatTS()));
                  }
                }))
        .apply(
            Window.<LatTSWrapped<Long>>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(configuration.windowSizeSec))))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO))
        .apply(Count.perElement())
        .apply(
            name + ".ToResult",
            ParDo.of(
                new DoFn<KV<LatTSWrapped<Long>, Long>, LatTSWrapped<BidsPerSession>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(LatTSWrapped.of(new BidsPerSession(c.element().getKey().getValue(), c.element().getValue()), c.element().getKey().getLatTS()));
                  }
                }));
  }
}
