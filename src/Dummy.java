import java.util.List;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

public class Dummy {

	public static void main(String[] args) {
		String logformat = "%h %l %u %t \"%r\" %>s %b";
		Parser<Object> dummyParser = new HttpdLoglineParser<Object>(Object.class, logformat);
		List<String> possiblePaths = dummyParser.getPossiblePaths();
		for (String path: possiblePaths) {
		    System.out.println(path);
		}

	}

}
