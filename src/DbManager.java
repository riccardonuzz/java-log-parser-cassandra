import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;

public class DbManager {
	Cluster cluster;
	Session session;
	
	protected DbManager() {
		try {
		    cluster = Cluster.builder()                                                    
		    		  .addContactPoint("127.0.0.1")
		              .build();
		    session = cluster.connect();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void insertAll(LogRecord record) {
		UUID timeBasedUuid = UUIDs.timeBased();
		int getReceivedBytes = 0;
		
		if(record.getValue("BYTESCLF:response.body.bytes") == null)
			getReceivedBytes = 0;
		else
			getReceivedBytes = Integer.parseInt(record.getValue("BYTESCLF:response.body.bytes"));
			
		Insert insert = QueryBuilder.insertInto("example", "log")
                .value("id", timeBasedUuid)
                .value("ip", record.getIP())
                .value("receivedbytes", getReceivedBytes)
                .value("requesturi", record.getValue("HTTP.URI:request.firstline.uri"))
                .value("timestamp", record.getValue("TIME.STAMP:request.receive.time"))
                .value("method", record.getValue("HTTP.METHOD:request.firstline.original.method"))
                .value("protocol", record.getValue("HTTP.PROTOCOL:request.firstline.protocol"))
                .value("protocolversion", record.getValue("HTTP.PROTOCOL.VERSION:request.firstline.protocol.version"))
                .value("requeststatus", record.getValue("STRING:request.status.last"));
		//System.out.println(insert.toString());
		session.execute(insert.toString());
	}
	
	protected List<Row> readAll() {
		ResultSet rs = session.execute("select * from example.log");    // (3)
	    //Row row = rs.one();
	    return rs.all();
	}
	
	protected boolean closeConnection() {
		if (cluster != null) {
			cluster.close();
			return true;
		}
		else {
			return false;
		}
	}
	
	
}
