/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.text.SimpleDateFormat;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ReportBolt extends BaseRichBolt {

	private FileWriter fileWriter1;
	private BufferedWriter bw1;
	private FileWriter fileWriter2;
	private BufferedWriter bw2;
	private int freq;
	static String type = null;
	long startTime = System.currentTimeMillis();
	HashMap<String, Integer> hm = new HashMap<String, Integer>();
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		try {
			bw1 = new BufferedWriter(new FileWriter("/s/chopin/a/grad/jcuomo/HashTagsP.csv", true));
		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
		startTime = System.currentTimeMillis();

	}

	public void execute(Tuple tuple) {
		
		String hashtag = tuple.getStringByField("hashtag");
		freq = tuple.getIntegerByField("freq");
		//System.out.println(hashtag);

		displayOutput(hashtag,freq);

	}
	
	public void displayOutput(String hashtag, int freq){
		long currentTime = System.currentTimeMillis();
		
		if(currentTime >= startTime + 10000 && !hm.isEmpty()){
			LinkedHashMap<String, Integer> lhm = sortHashMap(hm);
			
            LinkedHashMap<String, Integer> finalEmit = new LinkedHashMap<String, Integer>();
            int j=0;
			for (String name: lhm.keySet()){
            	finalEmit.put(name, lhm.get(name));
            	j++;
            	if(j==100) break;
             } 
       		System.out.println("BBfinal emit size"+finalEmit.size());
            //collector.emit(new Values(finalEmit.keySet().toString(), currentTime));
			
		
			Date resultdate = new Date(currentTime);
			try {
				bw1.write(dateFormat.format(resultdate) + "," + finalEmit.keySet().toString() + "\n");
				bw1.flush();
			} catch (Exception e) {
				e.printStackTrace();
			}
			hm.clear();
			startTime = currentTime;
		}
		else{
			if(!hm.containsKey(hashtag))
				hm.put(hashtag, freq);
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

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void cleanup() {
		try {
			bw1.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
