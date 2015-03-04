package ua.sasha.realtime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import ua.sasha.realtime.tools.Rankable;
import ua.sasha.realtime.tools.Rankings;

import java.util.Map;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {
    Rankings rankableList = (Rankings) tuple.getValue(0);

    for (Rankable r: rankableList.getRankings()){
      String word = r.getObject().toString();
      Long count = r.getCount();
      redis.publish("WordCountTopology", word + "|" + Long.toString(count));
    }

    // access the first column 'word'
    //String word = tuple.getStringByField("word");

    // access the second column 'count'
    //String word = rankedWords.toString();
    //Integer count = tuple.getIntegerByField("count");
    //Long count = new Long(100);

    // publish the word count to redis using word as the key
    //redis.publish("WordCountTopology", word + ":" + Long.toString(count));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
