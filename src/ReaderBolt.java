import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;



import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

@SuppressWarnings("serial")

public class ReaderBolt implements IRichBolt {
    private OutputCollector collector;
//    private FileWriter fileWriter;
//    private BufferedWriter bw;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//
//        try {
//            fileWriter = new FileWriter("/Users/Naveed/Desktop/twitter4jstorm/src/main/resources/HashTagLog.txt",true);
//            bw = new BufferedWriter(fileWriter);
//        } catch (Exception e) {
//            System.out.println("UNABLE TO WRITE FILE :: 1 ");
//            e.printStackTrace();
//        }

    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        for(HashtagEntity hashtage : tweet.getHashtagEntities()) {


                //System.out.println("Hashtag: " + hashtage.getText());


            this.collector.emit(new Values(hashtage.getText()));
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
