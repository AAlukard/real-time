package ua.sasha.realtime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * A bolt that counts the words that it receives
 */
public class CountBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the count of the words
  private Map<String, Long> countMap;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the map
    countMap = new HashMap<String, Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the word from the 1st column of incoming tuple
    String hashtag = tuple.getString(0);
    String tweet = tuple.getString(1);

    // check if the word is present in the map
    if (countMap.get(hashtag) == null) {

      // not present, add the word with a count of 1
      countMap.put(hashtag, 1L);
    } else {

      // already there, hence get the count
      Long val = countMap.get(hashtag);

      // increment the count and save it to the map
      countMap.put(hashtag, ++val);
    }

    // emit the word and count
    collector.emit(new Values(hashtag, countMap.get(hashtag), tweet));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("hashtag","count", "tweet"));
  }
}
