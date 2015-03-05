/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ua.sasha.realtime.ranking;

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import ua.sasha.realtime.AbstractRankerBolt;
import ua.sasha.realtime.ranking.tools.RankableTweet;
import ua.sasha.realtime.tools.Rankable;

//import storm.starter.tools.Rankable;
//import storm.starter.tools.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 */
public final class IntermediateTweetRankingsBolt extends AbstractRankerBolt {

  private static final long serialVersionUID = -1369800530256637409L;
  private static final Logger LOG = Logger.getLogger(IntermediateTweetRankingsBolt.class);

  public IntermediateTweetRankingsBolt() {
    super();
  }

  public IntermediateTweetRankingsBolt(int topN) {
    super(topN);
  }

  public IntermediateTweetRankingsBolt(int topN, int emitFrequencyInSeconds) {
    super(topN, emitFrequencyInSeconds);
  }

  @Override
  protected void updateRankingsWithTuple(Tuple tuple) {
    Rankable rankable = RankableTweet.from(tuple);
    super.getRankings().updateWith(rankable);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
