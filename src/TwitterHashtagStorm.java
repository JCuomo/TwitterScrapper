import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashtagStorm {
    public static void main(String[] args) throws Exception {

        String accessToken = "1105927449991274496-6CjiPkops1nOQDyTXdTwss7GeQDQS3";
        String accessTokenSecret = "i4nUp8uhx2wIfVzVUQ3us83Hx9wGTn24CVFMxdDe0633b";
        String consumerKey = "WTBu0qpuiiHNdvzgjl1dbAkqS";
        String consumerSecret = "H8WCvRD28g5RO5FwExRwqpeI7IPVkKoxpNRF90ObyYgOOvlnvh";

        //String[] arguments = args.clone();
        //String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        //String[] keyWords = {"zidane", "maduro"};
        String[] keyWords = {};

        Config config = new Config();
        //config.setDebug(true);
        config.put(Config.TOPOLOGY_DEBUG, false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout",
                new TwitterStormSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt()).shuffleGrouping("twitter-spout");

//        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
//                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        builder.setBolt("twitter-hashtag-counter-bolt", new LossyCounting(0.2, -1.0))
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
        
		builder.setBolt("Report-HashTag-Bolt", new OutputBolt()).globalGrouping("twitter-hashtag-counter-bolt");


        
        boolean runAsLocal = false;
        
        if (runAsLocal) {
        	LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("PA2", config, builder.createTopology());
        } else {
            StormSubmitter.submitTopology("PA2", config, builder.createTopology());
        }
        Thread.sleep(10000);
        //cluster.shutdown();
    }
}