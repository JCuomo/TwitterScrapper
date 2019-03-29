package old;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;



public class LossyCounting implements IRichBolt {


    private OutputCollector collector;
    private int Elements = 0;
    private Map<String, Entities> Mapwindow;
    private double epsilon;
    private double threshVal;

    private int Bucket = 1;
    private int BuckSize = (int) Math.ceil(1 / epsilon);
    private static long startTime;

    private long time;
    //SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public LossyCounting(double e, double threshold) {
        epsilon = e;
        threshVal = threshold;
    }


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        startTime = System.currentTimeMillis();
        this.Mapwindow = new ConcurrentHashMap<String, Entities>();
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getStringByField("hashtag");

 //       collector.ack(tuple);

        StreamCounting(key);

    }


    public void StreamCounting(String key) {

        if (Elements < BuckSize) {
            if (!Mapwindow.containsKey(key)) {
                Entities e = new Entities();
                e.delta = Bucket - 1;
                e.freq = 1;
                e.element = key;
                Mapwindow.put(key, e);
            } else {
                Entities e = Mapwindow.get(key);
                System.out.println("C  "+key+" "+Mapwindow.get(key).freq+" "+Mapwindow.get(key).element);
                e.freq += 1;
                Mapwindow.put(key, e);
            }
            Elements += 1;
        }
       // System.out.println("A  "+key+" "+Mapwindow.get(key).freq);
        
        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
        	
        	System.out.println("Printing Mapwindow");

            for (String name: Mapwindow.keySet()){
            	if(Mapwindow.get(name).freq > 1)
            		System.out.println(name.toString() + " " + Mapwindow.get(name).freq);  
            } 
        	System.out.println("finsih printing Mapwindow"+Mapwindow.size());

            
            if (!Mapwindow.isEmpty()) {
                HashMap<String, Integer> emittingList = new HashMap<String, Integer>();
                for (String str_loss : Mapwindow.keySet()) {
                    boolean b = check(str_loss);
                    //System.out.println(str_loss);
                    if (b) {
                        Entities e = Mapwindow.get(str_loss);
                        //System.out.println("B  "+str_loss+" "+e.freq);
                        emittingList.put(str_loss, e.freq);
                    }
                }
            	System.out.println("printing hash");
               for (String name: emittingList.keySet()){
            	   if(emittingList.get(name) > 1)
                        System.out.println(name.toString() + " " + emittingList.get(name));  
                } 
           		System.out.println("finsih printing hash"+emittingList.size());

           	
                if (!emittingList.isEmpty()) {


                    Collection<String> str;
                    /*if(link_Hash.size()>100){
                        str = Collections.list(Collections.enumeration(link_Hash.keySet())).subList(0, 100);
                    }
               		System.out.println("final emit size"+str.size());

                    str = (Collection<String>) link_Hash.keySet();
                    for(String s: str){
                        finalEmit.put(s, Mapwindow.get(s).freq);
                    }*/
                    LinkedHashMap<String, Integer> finalEmit = new LinkedHashMap<String, Integer>();
                    LinkedHashMap<String, Integer> link_Hash = sortHashMap(emittingList);
                    int j=0;
					for (String name: link_Hash.keySet()){
                    	finalEmit.put(name, Mapwindow.get(name).freq);
                    	j++;
                    	if(j==100) break;
                     } 
               		System.out.println("final emit size"+finalEmit.size());
                    collector.emit(new Values(finalEmit.keySet().toString(), currentTime));
                    //System.out.println("str:" + str.toString());
                }
            }
            startTime = currentTime;

        }
        if (BuckSize == Elements) {
            DeleteWord();
            Elements = 0;
            Bucket += 1;
        }
    }

    public LinkedHashMap<String, Integer> sortHashMap(HashMap<String, Integer> passedMap) {
        List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
        List<Integer> mapValues = new ArrayList<Integer>(passedMap.values());
        Collections.sort(mapValues, Collections.reverseOrder());
        Collections.sort(mapKeys, Collections.reverseOrder());

        LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

        Iterator<Integer> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Integer val = valueIt.next();
            Iterator<String> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                String key = keyIt.next();
                Integer comp1 = passedMap.get(key);
                Integer comp2 = val;

                if (comp1.equals(comp2)) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                }
            }
        }
        return sortedMap;
    }

    public boolean check(String lossyWord) {
        if(threshVal == -1.0){
            return true;
        }
        else{
            Entities d= Mapwindow.get(lossyWord);
            double a = (threshVal - epsilon) * Elements;
            if(d.freq >= a)
                return true;
            else
                return false;
        }
    }


    public void DeleteWord() {
        for (String key : Mapwindow.keySet()) {
            Entities e = Mapwindow.get(key);
            double sum = e.freq + e.delta;
            if (sum <= Bucket) {
                Mapwindow.remove(key);
            }
        }
    }


    @Override
    public void cleanup() {
        for(Map.Entry<String, Entities> entry:Mapwindow.entrySet()){
            System.out.println("Result: " + entry.getKey()+" : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("hashtag", "time"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}





