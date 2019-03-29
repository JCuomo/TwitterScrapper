/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/


import java.util.Date;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.text.SimpleDateFormat;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class OutputBolt extends BaseRichBolt {

	private FileWriter fileWriter1;
	private BufferedWriter bw1;
	private FileWriter fileWriter2;
	private BufferedWriter bw2;
	private long time;
	static String type = null;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		try {
			bw1 = new BufferedWriter(new FileWriter("/s/chopin/a/grad/jcuomo/HashTags.csv", true));

		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}

	}

	public void execute(Tuple tuple) {

		System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n\n\n\n\n\n\nAAAAAAAAAAAAAAAAaaa");
		String hashtag = tuple.getStringByField("hashtag");
		time = tuple.getLongByField("time");
		System.out.println(hashtag);
		//displayOutput(list);
		Date resultdate = new Date(time);
		try {

				bw1.write(dateFormat.format(resultdate) + "," + hashtag + "\n");
				bw1.flush();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * public void displayOutput(String list)
	 * 
	 * {
	 * 
	 * Date resultdate = new Date(time); try { if
	 * (type.equalsIgnoreCase("entity")) { bw2.write("<" +
	 * dateFormat.format(resultdate) + ">" + list + "\n"); bw2.flush(); } else {
	 * bw1.write("<" + dateFormat.format(resultdate) + ">" + list + "\n");
	 * bw1.flush(); } } catch (Exception e) { e.printStackTrace(); } }
	 */

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void cleanup() {
		try {
			bw1.close();
			bw2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
