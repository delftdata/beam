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

import javax.annotation.Nullable;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.IdNameReserve;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.workaround.LatTSWrapped;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Query 8, 'Monitor New Users'. Select people who have entered the system and created auctions in
 * the last 12 hours, updated every 12 hours. In CQL syntax:
 *
 * <pre>
 * SELECT Rstream(P.id, P.name, A.reserve)
 * FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
 * WHERE P.id = A.seller;
 * </pre>
 *
 * <p>To make things a bit more dynamic and easier to test we'll use a much shorter window.
 */
public class Query8 extends NexmarkQueryTransform<IdNameReserve> {
  private final NexmarkConfiguration configuration;

  public Query8(NexmarkConfiguration configuration) {
    super("Query8");
    this.configuration = configuration;
  }

  @Override
  public PCollection<LatTSWrapped<IdNameReserve>> expand(PCollection<LatTSWrapped<Event>> events) {
    // Window and key new people by their id.
    PCollection<KV<Long, LatTSWrapped<Person>>> personsById =
        events
            .apply(NexmarkQueryUtil.JUST_NEW_PERSONS)
            .apply(
                "Query8.WindowPersons",
                Window.into(FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec))))
            .apply("PersonById", NexmarkQueryUtil.PERSON_BY_ID);

    // Window and key new auctions by their id.
    PCollection<KV<Long, LatTSWrapped<Auction>>> auctionsBySeller =
        events
            .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)
            .apply(
                "Query8.WindowAuctions",
                Window.into(FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec))))
            .apply("AuctionBySeller", NexmarkQueryUtil.AUCTION_BY_SELLER);

    // Join people and auctions and project the person id, name and auction reserve price.
    return KeyedPCollectionTuple.of(NexmarkQueryUtil.PERSON_TAG, personsById)
        .and(NexmarkQueryUtil.AUCTION_TAG, auctionsBySeller)
        .apply(CoGroupByKey.create())
        .apply(
            name + ".Select",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, LatTSWrapped<IdNameReserve>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    @Nullable
                    LatTSWrapped<Person> person =
                        c.element().getValue().getOnly(NexmarkQueryUtil.PERSON_TAG, null);
                    if (person == null) {
                      // Person was not created in last window period.
                      return;
                    }
                    long maxTS = Long.MIN_VALUE;

                    for (LatTSWrapped<Auction> auction :
                            c.element().getValue().getAll(NexmarkQueryUtil.AUCTION_TAG))
                      maxTS = maxTS > auction.getLatTS() ? maxTS : auction.getLatTS();
                    for (LatTSWrapped<Auction> auction :
                        c.element().getValue().getAll(NexmarkQueryUtil.AUCTION_TAG)) {
                      c.output(LatTSWrapped.of(new IdNameReserve(person.getValue().id, person.getValue().name, auction.getValue().reserve), maxTS));
                    }
                  }
                }));
  }
}
