import twitter4j.RawStreamListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class twitter {
	
    public static void main(String[] args) throws TwitterException {
    	String accessToken = "1105927449991274496-6CjiPkops1nOQDyTXdTwss7GeQDQS3";
    	String accessTokenSecret = "i4nUp8uhx2wIfVzVUQ3us83Hx9wGTn24CVFMxdDe0633b";
    	String consumerKey = "WTBu0qpuiiHNdvzgjl1dbAkqS";
    	String consumerSecret = "H8WCvRD28g5RO5FwExRwqpeI7IPVkKoxpNRF90ObyYgOOvlnvh";
    	
    	TwitterStream twitterStream = new TwitterStreamFactory(
    			new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance();

    			twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
    			AccessToken token = new AccessToken(accessToken, accessTokenSecret);
    			twitterStream.setOAuthAccessToken(token);
    			
    			twitterStream.addListener(new RawStreamListener() {
    	            @Override
    	            public void onMessage(String rawJSON) {
    	                System.out.println(rawJSON);
    	            }

    	            @Override
    	            public void onException(Exception ex) {
    	                ex.printStackTrace();
    	            }
    	        }).sample();	
    }
}