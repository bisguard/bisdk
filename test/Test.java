import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

/**
 * Compile: javac -cp .../Test.java - see test.bat
 * Execute: java -cp Test  		    - see test.bat
 * Libraries:
 *  	httpasyncclient-4.1.2.jar
 *  	httpcore-nio-4.4.5.jar
 *  	httpcore-4.4.5.jar
 *  	commons-csv-1.4.jar
 *  	httpclient-4.5.2.jar
 *  	commons-cli-1.3.1.jar
 *  	commons-logging-1.2.jar
 *  	org.json.jar
 *  
 *  all jars excepts json can be downloaded
 *  from apache.org. json - from json.org
 *
 * Read csv file, finding 1000 most active users, 1000 most 
 * commented food items, 1000 most used words in the reviews,
 * and send the review texts to Google Translate API.
 * 
 * inner classes are used for reviewers convenience
 * 
 * @author Yury Bendersky
 * @version 0.9
 * @since January 26, 2017
 *
 */
public class Test
{
	static private Logger errorLog = Logger.getLogger(Test.class.getName());
	static private Handler fileHandler = null;

	private ThreadPool pool;

	final private long maxRAM = //500000000L;
								  4000000000L;
	final private long minAllowedMemory = 500000L;

	private List<Integer> hashCodes = new ArrayList<Integer>();
	private Map<String, Integer> activeUsers = new HashMap<String, Integer>();
	private Map<String, Integer> commentedFoodItems = new HashMap<String, Integer>();
	private Map<String, Integer> usedWords = new HashMap<String, Integer>();
	private Map<String, String> bufferForTranslate = Collections.synchronizedMap(new HashMap<String, String>());

	// params (hardcoded for the first rough test)
	private boolean translate = true;
	// google translate gives 404 and we use dummy host
	private String translateHost = "http://bisguard.com/test/translate.php";
	private boolean isMainMachine = true;
	private String[] remoteMachines = new String[0];
	// replace path for your csv file
	private String csvFile = "C:\\Workspaces\\Java\\Test\\csv\\Reviews.csv";
	
	public static void main(String[] args)
	{
		initLog();
		parseArgs(args);
		new Test().runTest();
	}

	private static void parseArgs(String[] args)
	{
		/* 
		 * we use the following args with apache commons-cli-1.3.1.jar
		 * 
		 * translate
		 * host for Translate API
		 * isMainMachine - PC used for csv file reading
		 * remoteMachines - for distributed computations
		 * csv file with Review records 
		 */
	}

	private static void initLog()
	{
		Formatter simpleFormatter;

		try
		{
			String path = new File(".").getCanonicalPath();
			String logName = String.valueOf(System.currentTimeMillis());
			simpleFormatter = new SimpleFormatter();
			fileHandler = new FileHandler(path + logName + ".log");
			fileHandler.setFormatter(simpleFormatter);
			fileHandler.setLevel(Level.ALL);
		}
		catch (IOException e)
		{
			errorLog.log(Level.WARNING, e.getMessage(), e);
		}
	}

	private void runTest()
	{
		checkThisMachineRAM();
		
		pool = new ThreadPool(4);

		pool.execute(new FreeMemoryTask());
		pool.execute(new ReadReviewsTask());

		if (translate)
		{
			pool.execute(new TranslateTask());
		}
		
		if (remoteMachines.length > 0)
		{
			pool.execute(new RemoteTask());

			if (Runtime.getRuntime().availableProcessors() > 4)
			{
				// add additional Tasks
			}
		}
		
		if (isMainMachine)
		{
			// message
		}
		else
		{
			// perform TranslateTask only
		}
	}

	private void checkThisMachineRAM()
	{
		long memorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
				.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();

		if (memorySize > maxRAM)
		{
			errorLog.log(Level.SEVERE, "RAM size " + memorySize + " > "
					+ maxRAM + " is not allowed.");
			// send message to RMI Service
			// ...
			exitAction();
		}
	}

	private void print1000s()
	{
		// print 1000 most active users (profile names)
		List<Pair> list = new ArrayList<Pair>();

		// copy Map to List for sorting
		Set<Map.Entry<String, Integer>> set = activeUsers.entrySet();
		Iterator<Entry<String, Integer>> iterator = set.iterator();

		while (iterator.hasNext())
		{
			Entry<String, Integer> entry = iterator.next();
			list.add(new Pair(entry.getKey(), entry.getValue()));
		}

		Collections.sort(list, new RatingComparator());

		List<Pair> alphaList = new ArrayList<Pair>();

		for (int i = 0; i < 1000; i++)
		{
			Pair p = list.get(i);
			alphaList.add(p);
			System.out.println("Name: " + p.name + " Rating: " + p.getRating());
		}

		Collections.sort(alphaList, new NameComparator());

		for (Pair p : alphaList)
		{
			System.out.println("Name: " + p.name + " Rating: " + p.getRating());
		}

		activeUsers.clear();
	}

	private void find1000s(CSVRecord csvRecord)
	{
		String key;
		
		// finding 1000 most active users (profile names)
		key = csvRecord.get("ProfileName");

		if (activeUsers.containsKey(key))
		{
			Integer val = activeUsers.get(key);
			activeUsers.put(key, ++val);
		}
		else
		{
			activeUsers.put(key, 1);
		}

		// finding 1000 most commented food items (item ids).
		key = csvRecord.get("ProductId");

		if (commentedFoodItems.containsKey(key))
		{
			Integer val = commentedFoodItems.get(key);
			commentedFoodItems.put(key, ++val);
		}
		else
		{
			commentedFoodItems.put(key, 1);
		}

		// finding 1000 most used words in the reviews
		String[] words = csvRecord.get("Text").split("\\b");

		for (String word : words)
		{
			if (usedWords.containsKey(word))
			{
				Integer val = usedWords.get(word);
				usedWords.put(word, ++val);
			}
			else
			{
				usedWords.put(word, 1);
			}
		}
	}

	private void exitAction()
	{
		if (fileHandler != null) fileHandler.close();
		errorLog.removeHandler(fileHandler);

		System.exit(1);

	}

	private class Pair
	{
		String name;
		Integer rating;

		public Pair(String key, Integer value)
		{
			name = key;
			rating = value;
		}

		String getName()
		{
			return name;
		}

		Integer getRating()
		{
			return rating;
		}
	}

	private class RatingComparator implements Comparator<Pair>
	{
		@Override
		public int compare(Pair p1, Pair p2)
		{
			return (p1.getRating() > p2.getRating() ? -1
					: (p1.getRating() == p2.getRating() ? 0 : 1));
		}
	}

	private class NameComparator implements Comparator<Pair>
	{
		@Override
		public int compare(Pair p1, Pair p2)
		{
			return p1.getName().compareTo(p2.getName());
		}
	}

	private class ReadReviewsTask implements Runnable
	{
		public void run()
		{
			readReviewRecords();
		}
		
		void readReviewRecords()
		{
			try
			{
				CSVParser parser = CSVFormat.EXCEL.withFirstRecordAsHeader()
											.parse(new FileReader(csvFile));

				int cnt = 0;
				
				for (CSVRecord csvRecord : parser)
				{
					String text = csvRecord.get("Text");
					if (text.length() > 1000) text = text.substring(0, 1000);
				
					// we are buffering texts for translation
					// in a buffer to avoid OutOfMemoryError
					bufferForTranslate.put(csvRecord.get("Id"), text);
					
					// prevent the processing of duplicate records
					String record = csvRecord.get("ProductId")
								+ csvRecord.get("UserId") + csvRecord.get("ProfileName")
								+ csvRecord.get("HelpfulnessNumerator")
								+ csvRecord.get("HelpfulnessDenominator")
								+ csvRecord.get("Score") + csvRecord.get("Time")
								+ csvRecord.get("Summary") + csvRecord.get("Text");

					Integer hash = record.hashCode();

					if (hashCodes.contains(hash))
					{
						errorLog.log(Level.WARNING, "Duplicate record found. Id = "
								+ csvRecord.get("Id"));
						continue;
					}

					hashCodes.add(hash);

					long freeMemory  = Runtime.getRuntime().freeMemory();
					
					if (freeMemory < minAllowedMemory || cnt++ > 1000)
					{
						cnt = 0;
						// test printing
						System.out.println(csvRecord);
						
						errorLog.log(Level.WARNING, "Free memory (bytes): " + freeMemory +
								" is too low");

//						we use everywhere hardcoded sleep() instead of 
//						pair wait/notify for first rough test only
						try
						{
							Thread.sleep(500);
						}
						catch (InterruptedException e)
						{
							errorLog.log(Level.WARNING, "", e);
						}
												
//						will be invoked by FreeMemoryTask						
//						synchronized (ReadReviewsTask.this)
//						{
//							try
//							{
//								ReadReviewsTask.this.wait();
//							}
//							catch (Exception e)
//							{
//								e.printStackTrace();
//							}
//						}
					}

					// statistics
					find1000s(csvRecord);
				}

				// print ratings
				print1000s();
			}
			catch (IOException e)
			{
				errorLog.log(Level.SEVERE, "", e);
				exitAction();
			}
		}		
	}

	// RemoteRask read texts from buffer
	// and perform TranslateTask on remote machine
	private class RemoteTask implements Runnable
	{
		public void run()
		{
			System.out.println("RemoteTask is running.");
			
			while (true)
			{
				sendToRemoteMachines();
			}
		}

		private void sendToRemoteMachines()
		{
			// we suppose to implement RMI here
			// suspend o low memory
		}
	}

	class TranslateTask implements Runnable
	{
		public void run()
		{
			while (true)
			{
				try
				{
					sendToGoogleTranslate();
				}
				catch (Exception e)
				{
					errorLog.log(Level.WARNING, "", e);
				}
				
//				we use everywhere hardcoded sleep() instead of 
//				pair wait/notify for first rough test only
				try
				{
					Thread.sleep(200);
				}
				catch (Exception e)
				{
					errorLog.log(Level.WARNING, "", e);
				}
			}
		}

		private void sendToGoogleTranslate() throws Exception
		{
			RequestConfig requestConfig = RequestConfig.custom()
					.setSocketTimeout(3000).setConnectTimeout(3000).build();
			CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
					.setDefaultRequestConfig(requestConfig).build();
			
			// for the first rough test only
			bufferForTranslate.clear();

			// read 100 texts from buffer (for the first rough test only)
			final HttpPost[] requests = new HttpPost[100];

			for (int i = 0; i < 100; i++)
			{
				JSONObject json = new JSONObject();
				json.put("input_lang", "en");
				json.put("output_lang", "fr");
				json.put("text", bufferForTranslate.get(i));

				requests[i] = new HttpPost(translateHost);
				requests[i].setEntity(new StringEntity(json.toString()));
				requests[i].addHeader("Content-type", "application/json");
				// for rollback we save id in the header
				requests[i].addHeader("Text-id", "" + i);
			}

			httpClient.start();
			
			try
			{
				final CountDownLatch latch = new CountDownLatch(requests.length);

				for (final HttpPost request : requests)
				{
					httpClient.execute(request, new FutureCallback<HttpResponse>()
					{
						@Override
						public void completed(HttpResponse response)
						{
							latch.countDown();

							if (response.getStatusLine().getStatusCode() == 200)
							{
								try
								{
									HttpEntity entity = response.getEntity();
									String content = EntityUtils.toString(entity);
									int id = Integer.parseInt(request.getFirstHeader("Text-id").getValue());
									// to restrict dumping
									if (id < 5)
									{
										System.out.println("Id: " + id + "  Translation: " + content);
									}
								}
								catch (Exception e)
								{
									errorLog.log(Level.WARNING, "", e);
								}
							}
							else
							{
								// print error message, return text to buffer
							}
						}

						@Override
						public void failed(Exception e)
						{
							latch.countDown();
							e.printStackTrace();
						}

						@Override
						public void cancelled()
						{
							latch.countDown();
							System.out.println(request.getRequestLine() + " cancelled");
						}
					});
				}
				
				latch.await();
			}
			finally
			{
				httpClient.close();
			}
		}
	}

	class FreeMemoryTask implements Runnable
	{
		long m;

		public void run()
		{
			while (true)
			{
				m = Runtime.getRuntime().freeMemory();
				System.out.println("Free memory size = " + m);

//				we use everywhere hardcoded sleep() instead of 
//				pair wait/notify for first rough test only
//				synchronized(FreeMemoryTask.this)
//				{
//					if (m > minAllowedMemory)
//					{
//						// resume reading csv file
//						// ...
//						FreeMemoryTask.this.notifyAll();
//					}
//					
//					System.out.println("Free memory size = " + m);
//				}
				
				try
				{
					Thread.sleep(1000);
				}
				catch (Exception e)
				{
					errorLog.log(Level.WARNING, "", e);
				}
			}
		}
	}

	private class ThreadPool
	{
		private final PoolWorker[] threads;
		private final LinkedBlockingQueue<Runnable> queue;

		public ThreadPool(int n)
		{
			queue = new LinkedBlockingQueue<Runnable>();
			threads = new PoolWorker[n];

			for (int i = 0; i < n; i++)
			{
				threads[i] = new PoolWorker();
				threads[i].start();
			}
		}

		public void execute(Runnable task)
		{
			synchronized (queue)
			{
				queue.add(task);
				queue.notify();
			}
		}

		private class PoolWorker extends Thread
		{
			public void run()
			{
				Runnable task;

				while (true)
				{
					synchronized (queue)
					{
						while (queue.isEmpty())
						{
							try
							{
								queue.wait();
							}
							catch (InterruptedException e)
							{
								errorLog.log(Level.WARNING, "", e);
							}
						}

						task = queue.poll();
					}

					try
					{
						task.run();
					}
					// to avoid leak threads
					catch (RuntimeException e)
					{
						errorLog.log(Level.WARNING, "", e);
					}
				}
			}
		}
	}
}
