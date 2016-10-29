import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Crawler implements Runnable
{


    String initialurl;
    //Queue<String> travUrl = new LinkedList<String>();
    HashSet<String> visited = new HashSet<String>();
    //Hashtable<String, Integer> depth = new Hashtable<String, Integer>();
    Queue<String> notvisited = new LinkedList<String>();


    //int maxdepth = 0;
    //int newdepth = 0;
    //int numvisited = 0;
   // Stack<String> stack = new Stack<String>();
    //Hashtable<String, Integer> freq = new Hashtable<String, Integer>();
    //int counter = 0;
    String current;

    String prepstmt = "INSERT INTO wordtable2(word,urlId) VALUES (?,?)";


	Connection connection;
	int urlID = 0;
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
            stat.executeUpdate("DROP TABLE URLS2");
            stat.executeUpdate("DROP TABLE WORDTABLE2");
        } catch (Exception e) {
        }

        // Create the table
        try {
            stat.executeUpdate("CREATE TABLE URLS2 (urlid INT auto_increment, url VARCHAR(512), description VARCHAR(200), primary key (urlid))");
            stat.executeUpdate("CREATE TABLE WORDTABLE2 (word VARCHAR(100), urlId INTEGER)");
            stat.execute("alter table urls2 auto_increment = 0; ");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
	}

	public synchronized boolean urlInDB(String urlFound) throws SQLException, IOException {
         	Statement stat = connection.createStatement();
		ResultSet result = stat.executeQuery( "SELECT * FROM urls2 WHERE url LIKE '"+urlFound+"'");

		if (result.next()) {
	        	System.out.println("URL "+urlFound+" already in DB");
			return true;
		}
	       // System.out.println("URL "+urlFound+" not yet in DB");
		return false;
	}
    String query;
	public synchronized void insertURLInDB( String url, String descr) throws SQLException, IOException {
         	Statement stat = connection.createStatement();
		 query = "INSERT INTO urls2(url,description) VALUES ('"+url+"','"+descr+"')";
        // query = "INSERT INTO urls1 VALUES ('"+urlID+"','"+url+"','" +descr+"')";
        //query = "INSERT INTO urls1 VALUES ('"+urlID+"','"+url+"','" +descr+"')";
        //System.out.println("Executing "+query);
		stat.executeUpdate( query );
		urlID++;
        //updatecurrUrlId(Integer.toString(urlID));
	}

	public synchronized void insertWordInDB(List<String> word)throws SQLException, IOException
    {
        //try {
            //Statement stat = connection.createStatement();
            //query = "INSERT INTO wordtable VALUES ('" + word + "','" + counter + "')";
            //System.out.println("Executing "+query);
            try {
                PreparedStatement ps = connection.prepareStatement(prepstmt);
                for (String x : word) {
                    ps.setString(1, x);
                    //System.out.println(Integer.toString(urlID));
                    ps.setString(2, Integer.toString(urlID));
                    ps.addBatch();
                }

                ps.executeBatch();
                ps.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
                System.out.println("I will not add");

            }
        //stat.executeUpdate(query);

    }

   // String newS;



//start bfs here
    //String yolo = "";
   // String parse = "";
String restring;
   // Queue<String>descr = new LinkedList<String>();
List<String> wordL = new ArrayList<String>();


    public synchronized void crawl()throws IOException, InterruptedException{
        //int currdepth = 0;
        //depth.put(initialurl, 0);
        notvisited.add(initialurl);

        int n = 0;
        while (n < 10000) {
            try {
                System.out.println(n);
                System.out.printf("Current link is %s \n", notvisited.element());
                current = notvisited.element();
                String oldUrl = notvisited.poll();
               // travUrl.add(oldUrl);
                oldUrl.replace(" ","%20");

                //Document doc = null;

                // if (!visited.contains(oldUrl)) {
                Document doc = Jsoup.connect(oldUrl).timeout(5000).get();
                Thread.sleep(1000);

                oldUrl = oldUrl.replaceAll("\\s+","%20");
                System.out.printf("Current oldUrl is %s \n", oldUrl);
                visited.add(oldUrl);
                //visited.put(oldUrl, 0);
                //depth.put(oldUrl,0);

                Elements links = doc.select("a[href]");

                for (Element link : links) {
                    String linkh = link.attr("abs:href");

                   // if (!travUrl.contains(linkh)) { // if not
                      if(!visited.contains(linkh)){
                        // traversed then
                        // added to
                        // queue
                        //counter++;
                        //travUrl.add(linkh);

                        notvisited.add(linkh);

                        //System.out.println(linkh);
                    }
                }

                int charcount = 0;
                 restring = "";
                String texta = doc.body().text();
                String newtext = texta.toLowerCase();
                //System.out.println(newtext);

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
                //String r = "";
                 a = newtext.split("\\P{Alpha}+");

                for (String x: a)
                {

                    // r = r + a + " ";
                    //insertWordInDB(x);
                    wordL.add(x);
               }




                insertURLInDB(oldUrl,restring);
                System.out.println("added url to DB");

                System.out.println("adding " + wordL.toString());
               insertWordInDB(wordL);
                wordL.clear();
                //descr.add(r);
                System.out.println("added word to DB");




                if(urlID == 10000)
                {
                    break;
                }

                // } else {
                //    visited.put(oldUrl, visited.get(oldUrl) + 1);
                //  counter++;

                // }
            } catch (Exception e) {
                n++;//added
                continue;
            }
            n++;

        }

    }




//end bfs here





public void run()
{
    try
    {
        crawl();
    }
    catch (Exception e) {}
}




   	public static void main(String[] args)throws IOException
   	{

        Crawler crawler = new Crawler();

        Thread t[] = new Thread[1];
        for(int i = 0; i < t.length; i++)
            t[i] = new Thread(crawler, "" + i);


        try {
			crawler.readProperties();
            //System.out.println(crawler.props.getProperty("crawler.urlId"));

            crawler.initialurl = crawler.props.getProperty("crawler.root");
            //crawler.urlID = Integer.parseInt(crawler.props.getProperty("crawler.urlId"));



			//String root = crawler.props.getProperty("crawler.root");

			crawler.createDB();

            System.out.println("Initializing Crawl Sequence...");

            //
             crawler.crawl();

          //  for(int i = 0; i < t.length; i++)
            //    t[i].start();

            //for(int i = 0; i < t.length; i++)
              //  t[i].join();

        }
		catch( Exception e) {
         		e.printStackTrace();
            System.out.println("fucked up here");
		}
    	}
}

