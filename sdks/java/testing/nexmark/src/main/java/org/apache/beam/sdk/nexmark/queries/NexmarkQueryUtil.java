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

import org.apache.beam.sdk.nexmark.model.*;
import org.apache.beam.sdk.nexmark.model.workaround.LatTSWrapped;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Utilities for working with NEXmark data stream.
 */
public class NexmarkQueryUtil {
    // Do not instantiate
    private NexmarkQueryUtil() {
    }

    public static final TupleTag<LatTSWrapped<Auction>> AUCTION_TAG = new TupleTag<>("auctions");
    public static final TupleTag<LatTSWrapped<Bid>> BID_TAG = new TupleTag<>("bids");
    public static final TupleTag<LatTSWrapped<Person>> PERSON_TAG = new TupleTag<>("person");

    /**
     * Predicate to detect a new person event.
     */
    public static final SerializableFunction<LatTSWrapped<Event>,Boolean> IS_NEW_PERSON =
            event -> event.getValue().newPerson != null;

    /**
     * DoFn to convert a new person event to a person.
     */
    public static final DoFn<LatTSWrapped<Event>, LatTSWrapped<Person>> AS_PERSON =
            new DoFn<LatTSWrapped<Event>, LatTSWrapped<Person>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(LatTSWrapped.of(c.element().getValue().newPerson, c.element().getLatTS()));
                }
            };

    /**
     * Predicate to detect a new auction event.
     */
    public static final SerializableFunction<LatTSWrapped<Event>, Boolean> IS_NEW_AUCTION =
            event -> event.getValue().newAuction != null;

    /**
     * DoFn to convert a new auction event to an auction.
     */
    public static final DoFn<LatTSWrapped<Event>, LatTSWrapped<Auction>> AS_AUCTION =
            new DoFn<LatTSWrapped<Event>, LatTSWrapped<Auction>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(LatTSWrapped.of(c.element().getValue().newAuction, c.element().getLatTS()));
                }
            };

    /**
     * Predicate to detect a new bid event.
     */
    public static final SerializableFunction<LatTSWrapped<Event>, Boolean> IS_BID =
            event -> event.getValue().bid != null;

    /**
     * DoFn to convert a bid event to a bid.
     */
    public static final DoFn<LatTSWrapped<Event>, LatTSWrapped<Bid>> AS_BID =
            new DoFn<LatTSWrapped<Event>, LatTSWrapped<Bid>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(LatTSWrapped.of(c.element().getValue().bid, c.element().getLatTS()));
                }
            };

    /**
     * Transform to key each person by their id.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Person>, KV<Long, LatTSWrapped<Person>>> PERSON_BY_ID =
            ParDo.of(
                    new DoFn<LatTSWrapped<Person>, KV<Long, LatTSWrapped<Person>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(KV.of(c.element().getValue().id, c.element()));
                        }
                    });

    /**
     * Transform to key each auction by its id.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Auction>, KV<Long, LatTSWrapped<Auction>>> AUCTION_BY_ID =
            ParDo.of(
                    new DoFn<LatTSWrapped<Auction>, KV<Long, LatTSWrapped<Auction>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(KV.of(c.element().getValue().id, c.element()));
                        }
                    });

    /**
     * Transform to key each auction by its seller id.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Auction>, KV<Long, LatTSWrapped<Auction>>> AUCTION_BY_SELLER =
            ParDo.of(
                    new DoFn<LatTSWrapped<Auction>, KV<Long, LatTSWrapped<Auction>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(KV.of(c.element().getValue().seller, c.element()));
                        }
                    });

    /**
     * Transform to key each bid by it's auction id.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Bid>, KV<Long, LatTSWrapped<Bid>>> BID_BY_AUCTION =
            ParDo.of(
                    new DoFn<LatTSWrapped<Bid>, KV<Long, LatTSWrapped<Bid>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(KV.of(c.element().getValue().auction, c.element()));
                        }
                    });

    /**
     * Transform to project the auction id from each bid.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Bid>, LatTSWrapped<Long>> BID_TO_AUCTION =
            ParDo.of(
                    new DoFn<LatTSWrapped<Bid>, LatTSWrapped<Long>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(LatTSWrapped.of(c.element().getValue().auction, c.element().getLatTS()));
                        }
                    });

    /**
     * Transform to project the price from each bid.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Bid>, LatTSWrapped<Long>> BID_TO_PRICE =
            ParDo.of(
                    new DoFn<LatTSWrapped<Bid>, LatTSWrapped<Long>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(LatTSWrapped.of(c.element().getValue().price, c.element().getLatTS()));
                        }
                    });

    /**
     * Transform to emit each event with the timestamp embedded within it.
     */
    public static final ParDo.SingleOutput<LatTSWrapped<Event>, LatTSWrapped<Event>> EVENT_TIMESTAMP_FROM_DATA =
            ParDo.of(
                    new DoFn<LatTSWrapped<Event>, LatTSWrapped<Event>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            Event e = c.element().getValue();
                            if (e.bid != null) {
                                c.outputWithTimestamp(LatTSWrapped.of(e, c.element().getLatTS()),
                                        new Instant(e.bid.dateTime));
                            } else if (e.newPerson != null) {
                                c.outputWithTimestamp(LatTSWrapped.of(e, c.element().getLatTS()),
                                        new Instant(e.newPerson.dateTime));
                            } else if (e.newAuction != null) {
                                c.outputWithTimestamp(LatTSWrapped.of(e, c.element().getLatTS()),
                                        new Instant(e.newAuction.dateTime));
                            }
                        }
                    });

    /**
     * Transform to filter for just the new auction events.
     */
    public static final PTransform<PCollection<LatTSWrapped<Event>>, PCollection<LatTSWrapped<Auction>>> JUST_NEW_AUCTIONS =
            new PTransform<PCollection<LatTSWrapped<Event>>, PCollection<LatTSWrapped<Auction>>>("justNewAuctions") {
                @Override
                public PCollection<LatTSWrapped<Auction>> expand(PCollection<LatTSWrapped<Event>> input) {
                    return input
                            .apply("IsNewAuction", Filter.by(IS_NEW_AUCTION))
                            .apply("AsAuction", ParDo.of(AS_AUCTION));
                }
            };


    /**
     * Transform to filter for just the new person events.
     */
    public static final PTransform<PCollection<LatTSWrapped<Event>>, PCollection<LatTSWrapped<Person>>> JUST_NEW_PERSONS =
            new PTransform<PCollection<LatTSWrapped<Event>>, PCollection<LatTSWrapped<Person>>>("justNewPersons") {
                @Override
                public PCollection<LatTSWrapped<Person>> expand(PCollection<LatTSWrapped<Event>> input) {
                    return input
                            .apply("IsNewPerson", Filter.by(IS_NEW_PERSON))
                            .apply("AsPerson", ParDo.of(AS_PERSON));
                }
            };

    /**
     * Transform to filter for just the bid events.
     */
    public static final PTransform<PCollection<LatTSWrapped<Event>>, PCollection<LatTSWrapped<Bid>>> JUST_BIDS =
            new PTransform<PCollection<LatTSWrapped<Event>>, PCollection<LatTSWrapped<Bid>>>("justBids") {
                @Override
                public PCollection<LatTSWrapped<Bid>> expand(PCollection<LatTSWrapped<Event>> input) {
                    return input.apply("IsBid", Filter.by(IS_BID)).apply("AsBid", ParDo.of(AS_BID));
                }
            };
}
