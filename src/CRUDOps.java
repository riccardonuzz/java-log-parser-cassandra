import java.util.List;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CRUDOps extends DbManager{
	public CRUDOps() {
		super();
	}
	
	public List<Row> getByResponseCode() {
		ResultSet rs = session.execute("SELECT * FROM example.log WHERE requeststatus='200' ALLOW FILTERING");    // (3)
	    //Row row = rs.one();
	    return rs.all();
	}
	
	public List<Row> getByResponseCodeAndProtocolVersion() {
		ResultSet rs = session.execute("SELECT * FROM example.log WHERE requeststatus='200' AND protocolversion='1.1' ALLOW FILTERING");    // (3)
	    return rs.all();
	}
	
	public List<Row> getRequestUriSubstring() { //twiki
		ResultSet rs = session.execute("SELECT * FROM example.log WHERE requesturi LIKE '/twiki%'");    // (3)
	    return rs.all();
	}
	
	public List<Row> getBytesReceivedGreaterThan() {
		ResultSet rs = session.execute("SELECT * FROM example.log WHERE receivedbytes > 1111 ALLOW FILTERING");    // (3)
	    return rs.all();
	}
	
	public List<Row> getURIWithReceivedBytesGreaterThan() {
		ResultSet rs = session.execute("SELECT requesturi FROM example.log WHERE receivedbytes > 1111 ALLOW FILTERING");    // (3)
	    return rs.all();
	}
	
	public List<Row> getLimitedURIWithReceivedBytesGreaterThan() {
		ResultSet rs = session.execute("SELECT requesturi FROM example.log WHERE receivedbytes > 1111 ALLOW FILTERING");    // (3)
	    return rs.all();
	}
	
	public Row getCountURIWithReceivedBytesGreaterThan() {
		ResultSet rs = session.execute("SELECT count(*) FROM example.log WHERE receivedbytes > 1111 ALLOW FILTERING");    // (3)
	    return rs.one();
	}
}
