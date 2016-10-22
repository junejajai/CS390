import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Crawler
{


    String initialurl = "http://purdue.edu";
    Queue<String> travUrl = new LinkedList<String>();
    Hashtable<String, Integer> visited = new Hashtable<String, Integer>();
    //Hashtable<String, Integer> depth = new Hashtable<String, Integer>();
    Queue<String> notvisited = new LinkedList<String>();
    int maxdepth = 0;
    int newdepth = 0;
    int numvisited = 0;
   // Stack<String> stack = new Stack<String>();
    Hashtable<String, Integer> freq = new Hashtable<String, Integer>();
    int counter = 0;




	Connection connection;
	int urlID;
	public Properties props;

	Crawler() {
		urlID = 0;
	}

	public void readProperties() throws IOException {
      		props = new Properties();
      		FileInputStream in = new FileInputStream("src/database.properties");
      		props.load(in);
      		in.close();
	}

	public void openConnection() throws SQLException, IOException
	{
		String drivers = props.getProperty("jdbc.drivers");
      		if (drivers != null) System.setProperty("jdbc.drivers", drivers);

      		String url = props.getProperty("jdbc.url");
      		String username = props.getProperty("jdbc.username");
      		String password = props.getProperty("jdbc.password");

		connection = DriverManager.getConnection( url, username, password);
   	}

	public void createDB() throws SQLException, IOException {
		openConnection();

         	Statement stat = connection.createStatement();
		
		// Delete the table first if any
		try {
			stat.executeUpdate("DROP TABLE URLS");
		}
		catch (Exception e) {
		}
			
		// Create the table
        	stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200))");
	}

	public boolean urlInDB(String urlFound) throws SQLException, IOException {
         	Statement stat = connection.createStatement();
		ResultSet result = stat.executeQuery( "SELECT * FROM urls WHERE url LIKE '"+urlFound+"'");

		if (result.next()) {
	        	System.out.println("URL "+urlFound+" already in DB");
			return true;
		}
	       // System.out.println("URL "+urlFound+" not yet in DB");
		return false;
	}

	public void insertURLInDB( String url) throws SQLException, IOException {
         	Statement stat = connection.createStatement();
		String query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','')";
		//System.out.println("Executing "+query);
		stat.executeUpdate( query );
		urlID++;
	}

/*
	public String makeAbsoluteURL(String url, String parentURL) {
		if (url.indexOf(":")<0) {
			// the protocol part is already there.
			return url;
		}

		if (url.length > 0 && url.charAt(0) == '/') {
			// It starts with '/'. Add only host part.
			int posHost = url.indexOf("://");
			if (posHost <0) {
				return url;
			}
			int posAfterHist = url.indexOf("/", posHost+3);
			if (posAfterHist < 0) {
				posAfterHist = url.Length();
			}
			String hostPart = url.substring(0, posAfterHost);
			return hostPart + "/" + url;
		} 

		// URL start with a char different than "/"
		int pos = parentURL.lastIndexOf("/");
		int posHost = parentURL.indexOf("://");
		if (posHost <0) {
			return url;
		}
		
		
		

	}
*/
/*
   	public void fetchURL(String urlScanned) {
		try {
			URL url = new URL(urlScanned);
			System.out.println("urlscanned="+urlScanned+" url.path="+url.getPath());
 
    			// open reader for URL
    			InputStreamReader in = 
       				new InputStreamReader(url.openStream());

    			// read contents into string builder
    			StringBuilder input = new StringBuilder();
    			int ch;
			while ((ch = in.read()) != -1) {
         			input.append((char) ch);
			}

     			// search for all occurrences of pattern
    			String patternString =  "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]*)\\s*>";
    			Pattern pattern = 			
	     			Pattern.compile(patternString, 
	     			Pattern.CASE_INSENSITIVE);
    			Matcher matcher = pattern.matcher(input);
		
			while (matcher.find()) {
    				int start = matcher.start();
    				int end = matcher.end();
    				String match = input.substring(start, end);
				String urlFound = matcher.group(1);
				System.out.println(urlFound);

				// Check if it is already in the database
				if (!urlInDB(urlFound)) {
					insertURLInDB(urlFound);
				}				
	
    				//System.out.println(match);
 			}

		}
      		catch (Exception e)
      		{
       			e.printStackTrace();
      		}
	}*/




//start bfs here



Queue<String>descr = new LinkedList<String>();

    public void crawl()throws IOException{
        int currdepth = 0;
        //depth.put(initialurl, 0);
        notvisited.add(initialurl);
        int n = 0;
        while (n < 20) {
            try {
                System.out.println(n);
                System.out.printf("Current link is %s \n", notvisited.element());
                String oldUrl = notvisited.poll();
                travUrl.add(oldUrl);
                //Document doc = null;

                // if (!visited.contains(oldUrl)) {
                Document doc = Jsoup.connect(oldUrl).get();
                //visited.put(oldUrl, 0);
                //depth.put(oldUrl,0);

                Elements links = doc.select("a[href]");

                for (Element link : links) {
                    String linkh = link.attr("abs:href");

                    if (travUrl.contains(linkh) == true) { // if not
                        // traversed then
                        // added to
                        // queue

                        //counter++;
                    } else {
                        //System.out.println("already added this to list");
                        //visited.put(linkh, visited.get(linkh) + 1);
                        //System.out.println("Insetred and updated");
                       // currdepth = depth.get(oldUrl) + 1;





                        travUrl.add(linkh);




                        // System.out.println("CurrDerpth" + currdepth);
                       // if (notvisited.contains(linkh) == false)
                           // depth.put(linkh, depth.get(oldUrl) + 1);
                       // if (maxdepth <= currdepth) {
                        //    maxdepth = currdepth;
                        //}


                        // visited.put(linkh, 1);
                        //if (!linkh.contains(".pdf")
                        //      && !linkh.contains(".ico")) { //add these regardless
                        notvisited.add(linkh);

                        System.out.println(linkh);

                    }

                }

                int charcount = 0;
                String restring = "";
                String texta = doc.body().text();
                String newtext = texta.toLowerCase();
                String a[] = newtext.split("\\P{Alpha}+");

                for(String x : a)
                {
                    if(charcount>=100)
                    {
                        break;
                    }
                    charcount+= x.length();
                    restring = restring + x + " ";

                }

                descr.add(restring);
                System.out.println(restring);
                insertURLInDB(oldUrl);


                // } else {
                //    visited.put(oldUrl, visited.get(oldUrl) + 1);
                //  counter++;

                // }
            } catch (Exception e) {
                //n++;//added
                continue;
            }
            n++;

        }
        //System.out.println(visited.toString());
       // System.out.println(notvisited.size());
       // System.out.println("Maxdepth is : " + maxdepth);


        //System.out.println("Counter is : " + (counter));

        //System.out.println(depth.toString());
    }




//end bfs here










   	public static void main(String[] args)throws IOException
   	{

        Crawler crawler = new Crawler();


		try {
			crawler.readProperties();
			String root = crawler.props.getProperty("crawler.root");
			crawler.createDB();
            crawler.crawl();
			//crawler.fetchURL(root);


		}
		catch( Exception e) {
         		e.printStackTrace();
		}
    	}
}

