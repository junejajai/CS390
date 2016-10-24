import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class Crawler
{


    String initialurl;
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
    String current;



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
            stat.executeUpdate("DROP TABLE WORDTABLE");
		}
		catch (Exception e) {
		}
			
		// Create the table
        	stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200))");
            stat.executeUpdate("CREATE TABLE WORDTABLE (word VARCHAR(512), urlId INTEGER)");
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
    String query;
	public void insertURLInDB( String url, String descr) throws SQLException, IOException {
         	Statement stat = connection.createStatement();
		//String query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','')";
         query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','" +descr+"')";
        //System.out.println("Executing "+query);
		stat.executeUpdate( query );
		urlID++;
	}

	public void insertWordInDB(String word)throws SQLException, IOException
    {
        //try {
            Statement stat = connection.createStatement();
            //String query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','')";
            query = "INSERT INTO wordtable VALUES ('" + word + "','" + counter + "')";
            //System.out.println("Executing "+query);
            stat.executeUpdate(query);
        //}
        //catch (Exception e)
        //{
            //updatecurrUrl(current);
            //String urlD = ""+urlID;
          //  updatecurrUrlId(urlD);
        //}
    }

    String newS;
    public void updatecurrUrl(String currUrl) throws IOException {
        FileOutputStream out = new FileOutputStream("src/database.properties");
        newS = "";
        for(int i = 0; i<currUrl.length();i++)
        {
            if(currUrl.charAt(i)==':')
            {
                newS+="\\";
            }
            newS+=currUrl.charAt(i);
        }

        props.setProperty("crawler.currurl",newS);
        props.store(out, null);
        out.close();
    }

    public void updatecurrUrlId(String urlsId) throws IOException {
        FileOutputStream out = new FileOutputStream("src/database.properties");
        props.setProperty("crawler.urlId",urlsId);
        props.store(out, null);
        out.close();
    }


//start bfs here
    String yolo = "";
    
    String nparse[];
Queue<String>descr = new LinkedList<String>();

    public void crawl()throws IOException{
        int currdepth = 0;
        //depth.put(initialurl, 0);
        notvisited.add(initialurl);
        int n = 0;
        while (n < 10000) {
            try {
                System.out.println(n);
                System.out.printf("Current link is %s \n", notvisited.element());
                current = notvisited.element();
                String oldUrl = notvisited.poll();
                travUrl.add(oldUrl);
                //Document doc = null;

                // if (!visited.contains(oldUrl)) {
                Document doc = Jsoup.connect(oldUrl).timeout(5000).get();
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


                        travUrl.add(linkh);

                        notvisited.add(linkh);

                        System.out.println(linkh);

                    }

                }

                int charcount = 0;
                String restring = "";
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
                String r = "";
                 a = newtext.split("\\P{Alpha}+");
                for (String x: a)
                {
                    r = r + a + " ";
                 //   insertWordInDB(x);
               }


                descr.add(r);

                if(descr.size() == 200)
                {
                    while(!descr.isEmpty())
                    {
                        try {
                        yolo = descr.poll();
                        nparse = yolo.split("\\P{Alpha}+");

                        //for(parse:nparse)
                        for(String parse : nparse)
                        {
                            insertWordInDB(parse);
                        }
                        counter++;
                        }
                        catch(Exception e)
                        {
                            e.printStackTrace();
                        }

                    }
                    //while(descr.size() != 0) {
                      //  yolo = descr.poll();

                       // for (parse : yolo) {
                         //   insertWordInDB(parse);
                        //}

                        //counter++;
                    //}
                    }

                System.out.println(restring);
                insertURLInDB(oldUrl,restring);



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
            //System.out.println(crawler.props.getProperty("crawler.urlId"));

            crawler.initialurl = crawler.props.getProperty("crawler.currurl");
            crawler.urlID = Integer.parseInt(crawler.props.getProperty("crawler.urlId"));



			//String root = crawler.props.getProperty("crawler.root");
			crawler.createDB();
            crawler.crawl();
			//crawler.fetchURL(root);


		}
		catch( Exception e) {
         		e.printStackTrace();
            System.out.println("fucked up here");
		}
    	}
}

