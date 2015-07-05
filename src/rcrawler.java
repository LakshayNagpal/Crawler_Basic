import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

//import lakshay.webcrawler.com.SpiderLeg;


//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class rcrawler
extends Reducer<Text, Text, Text, Text> {
	
	public static final int MAX_PAGES_TO_SEARCH = 500;
	public Set<String> pagesVisited = new HashSet<String>();
	public List<String> pagesToVisit = new LinkedList<String>();
	public static final String USER_AGENT =
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";
    public List<String> links = new LinkedList<String>();
    public Document htmlDocument;	
	
/*
 * This function is to search for the word and return the word and the url on which the word is been found.
 * At the end, the function also returns statement when it is done with the process. 
 * 
 */
public String search(String url, String searchWord){
		
	

		String CurrentUrl =url;
			rcrawler leg = new rcrawler();
			
			leg.crawl(CurrentUrl);
			
	int success = leg.searchForWord(searchWord);
	this.pagesToVisit.addAll(leg.getLinks());
	
			if(success>0){
				return String.format("Success, Word %s found %d", searchWord,success);
			}
			
			
			return String.format("Failed, Word %s not found %d", searchWord,success);
			
	}	
	

/*
 * This function return the next url to be searched for the above function to perform
 * 
 */


 private String nextUrl()
{
    String nextUrl;
    do
    {
        nextUrl = this.pagesToVisit.remove(0);
    } while(this.pagesVisited.contains(nextUrl));
    this.pagesVisited.add(nextUrl);
    return nextUrl;
}

/*
 * This function gives the http requests and it also shows that how many links it has found on the partu=icular url
 * 
 */
public boolean crawl(String url)
{
    try
    {
        Connection connection = Jsoup.connect(url).userAgent(USER_AGENT);
        Document htmlDocument = connection.get();
        this.htmlDocument = htmlDocument;
     
        if(!connection.response().contentType().contains("text/html"))
        {
            return false;
        }
        
         Elements linksOnPage = htmlDocument.select("a[href]");
       
        for(Element link : linksOnPage)
        {
            this.links.add(link.absUrl("href"));
        }
        
        return true;
        
    }
    catch(IOException ioe)
    {
        
        return false;
    }
}

/*
 * This is the main fuction that looks for the word in the whole page and returns the value to the first function search
 */
public int searchForWord(String searchWord)
{
    // Defensive coding. This method should only be used after a successful crawl.
    if(this.htmlDocument == null)
    {
        
        return 0;
    }
    
    String bodyText = this.htmlDocument.body().text();
    return StringUtils.countMatches(bodyText.toLowerCase(),searchWord.toLowerCase());
   
}
/*
 * This function is to return all the links that the url consists of
 */

public List<String> getLinks()
{
    return this.links;
}



@Override
public void reduce(Text Key, Iterable<Text>values, Context context)
throws IOException, InterruptedException {
	
	for(Text value : values ) {
	rcrawler spider = new rcrawler();
	String Key1 = Key.toString(); String value1 = value.toString();
	
	while(spider.pagesVisited.size() < MAX_PAGES_TO_SEARCH)
	{
	
		if(spider.pagesToVisit.isEmpty())
		{
			spider.pagesVisited.add(Key1);
		}
		else
		{
			Key1=spider.nextUrl();
		}
			
	String result = spider.search(Key1, value1);
	 Text result1=new Text (result);
	      int r;
	       if(result.startsWith("S"))
	    	   
	    	       r=1;
	       else r=0;
	       String str = Integer.toString(r);
	     Text r1=new Text(str);
	context.write(result1, r1);
			}
	
		}
    }	
}