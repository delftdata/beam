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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.workaround.LatTSWrapped;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Query that joins a stream to a bounded side input, modeling basic stream enrichment.
 *
 * <pre>
 * SELECT bid.*, sideInput.*
 * FROM bid, sideInput
 * GROUP BY SESSION(bid.bidtime, bid.auctionid, config.sessionGap)
 * WHERE bid.id = sideInput.id
 * </pre>
 */
public class SessionSideInputJoin extends NexmarkQueryTransform<Bid> {
  private final NexmarkConfiguration configuration;

  public SessionSideInputJoin(NexmarkConfiguration configuration) {
    super("BoundedSideInputJoin");
    this.configuration = configuration;
  }

  @Override
  public boolean needsSideInput() {
    return true;
  }

  @Override
  public PCollection<LatTSWrapped<Bid>> expand(PCollection<LatTSWrapped<Event>> events) {

    checkState(getSideInput() != null, "Configuration error: side input is null");

    final PCollectionView<Map<Long, String>> sideInputMap = getSideInput().apply(View.asMap());

    return events
        // Only want the bid events; easier to fake some side input data
        .apply(NexmarkQueryUtil.JUST_BIDS)

        // Sessionize on bidtime keyed by bidder id (they might bid on multiple auctions)
        .apply(WithKeys.of(new SimpleFunction<LatTSWrapped<Bid>, Long>(bid -> bid.getValue().bidder) {}))
        .apply(Window.into(Sessions.withGapDuration(configuration.sessionGap)))
        .apply(GroupByKey.create())

        // Enrich each bid in the session with the joined data
        // concatenated with the session bounds
        .apply(
            name + ".JoinToFiles",
            ParDo.of(
                    new DoFn<KV<Long, Iterable<LatTSWrapped<Bid>>>, LatTSWrapped<Bid>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c, BoundedWindow untypedWindow) {
                        IntervalWindow window = (IntervalWindow) untypedWindow;

                        String extra =
                            c.sideInput(sideInputMap)
                                .get(c.element().getKey() % configuration.sideInputRowCount);

                        long maxTS = Long.MIN_VALUE;

                        for (LatTSWrapped<Bid> wrappedBid : c.element().getValue())
                          maxTS = Math.max(maxTS, wrappedBid.getLatTS());


                        for (LatTSWrapped<Bid> wrappedBid : c.element().getValue()) {
                          Bid bid = wrappedBid.getValue();

                          c.output(LatTSWrapped.of(
                              new Bid(
                                  bid.auction,
                                  bid.bidder,
                                  bid.price,
                                  bid.dateTime,
                                  extra + ":" + window.start() + ":" + window.end()), maxTS));
                        }
                      }
                    })
                .withSideInputs(sideInputMap));
  }
}
