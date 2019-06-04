// From https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-java-how-to-use-topics-subscriptions

package listener;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.google.gson.Gson;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class CalcbenchFilingsServiceBusTopicClient {

    static final Gson GSON = new Gson();
    
	public static void main(String[] args) throws Exception, ServiceBusException {
		Properties prop = new Properties();
		InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
		prop.load(input); 
		String connectionString = prop.getProperty("ConnectionString");
		String topicString = prop.getProperty("Topic");
        SubscriptionClient subscription1Client = new SubscriptionClient(new ConnectionStringBuilder(connectionString, topicString), ReceiveMode.PEEKLOCK);

        registerMessageHandlerOnClient(subscription1Client);
	}
	
    static void registerMessageHandlerOnClient(SubscriptionClient receiveClient) throws Exception {

        // register the RegisterMessageHandler callback
    	IMessageHandler messageHandler = new IMessageHandler() {
            // callback invoked when the message handler loop has obtained a message
            public CompletableFuture<Void> onMessageAsync(IMessage message) {

                byte[] body = message.getBody();
            	Filing filing =  GSON.fromJson(new String(body, UTF_8), Filing.class);
            	System.out.printf("filing received for %s\n", filing.ticker);
                return receiveClient.completeAsync(message.getLockToken());
            }
            
            public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                System.out.printf(exceptionPhase + "-" + throwable.getMessage());
            }
        };

 
        receiveClient.registerMessageHandler(
        			messageHandler,
                    // callback invoked when the message handler has an exception to report
                // 1 concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));

    }
}