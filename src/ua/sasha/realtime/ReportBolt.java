package ua.sasha.realtime;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.apache.log4j.Logger;
import ua.sasha.realtime.ranking.tools.RankableTweet;
import ua.sasha.realtime.tools.Rankable;
import ua.sasha.realtime.tools.RankableObjectWithFields;
import ua.sasha.realtime.tools.Rankings;

import java.util.Map;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(ReportBolt.class);
    // place holder to keep the connection to redis
    transient RedisConnection<String, String> redis;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost", 6379);

        // initiate the actual connection
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        Rankings rankableList = (Rankings) tuple.getValue(0);

        for (Rankable r : rankableList.getRankings()) {
            String word = r.getObject().toString();
            Long count = r.getCount();
            String text = (String) (((RankableTweet) r).getTweet());

            redis.publish("WordCountTopology", text + "|" + Long.toString(count));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }
}
