package ua.sasha.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


class TopNTweetTopology
{
  public static void main(String[] args) throws Exception
  {
    //Variable TOP_N number of words
    int TOP_N = 10;
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    TweetSpout tweetSpout = new TweetSpout(
        "tEd0wagtb0USE9X5DQEA",
        "UfF0vrUwvo8tzGtXw1KFm5ZkgWE7bCK9FClViOAF2KQ",
        "1421353664-8naTIBPcOfFuuMKdZ0unHjfJR8kzb9NfxUqRRY5",
        "Z3YhsxbAYSIGHyki3EoEOpE6NIUPr2xUB9FKsh4d3b4Go"

    );

    builder.setSpout("tweet-spout", tweetSpout, 1);

    builder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");

    builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("parse-tweet-bolt", new Fields("hashtag"));

    builder.setBolt("intermediate-ranker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("count-bolt", new Fields("hashtag"));

    builder.setBolt("total-ranker", new TotalRankingsBolt(TOP_N), 1).globalGrouping("intermediate-ranker");

    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("total-ranker");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
//    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
