import java.io.File;
import java.util.List;
import java.util.Scanner;

import com.datastax.driver.core.Row;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

public class TextualGUI {
	CRUDOps crudOps;
	
	public TextualGUI() {
		crudOps = new CRUDOps();
	}
	
	public void mainMenu() throws InvalidDissectorException, MissingDissectorsException, NoSuchMethodException, DissectionFailure{
		Scanner input = new Scanner(System.in);
		System.out.println("MENU PRINCIPALE\n" +
						   "Azioni possibili:\n" +
						   "1) Inserisci dati dal log al DB;\n" +
						   "2) Visualizza dati presenti nel DB;\n" +
						   "3) Visualizza righe del log in cui il codice di risposta e' 200;\n" +
						   "4) Visualizza righe del log in cui il codice di risposta e' 200 e la versione del protocollo HTTP e' 1.1;\n" +
						   "5) Visualizza righe del log in cui l'URI richiesto contiene 'twiki' nel nome;\n" +
						   "6) Visualizza righe del log in cui i bytes ricevuti sono maggiori di 1111;\n" +
						   "7) Visualizza gli URI in cui i bytes ricevuti sono maggiori di 1111;\n" +
						   "8) Visualizza solo 10 degli URI in cui i bytess ricevuti sono maggiori di 1111;\n" +
						   "9) Visualizza il numero di URI in cui i bytes ricevuti sono maggiori di 1111.\n"
		);
		
		System.out.print("Elemento selezionato: ");
		this.choice(Integer.parseInt(input.nextLine()));
		crudOps.closeConnection();
		input.close();
	}
	
	private void choice(int choice) {
		switch (choice) {
			case 1: this.insertAllData(); break;
			case 2: case 3: case 4: case 5: case 6: this.showSelectedData(choice); break;
			case 7: this.showGetURIWithReceivedBytesGreaterThan(); break;
			case 8: this.showGetLimitedURIWithReceivedBytesGreaterThan(); break;
			case 9: this.getCountURIWithReceivedBytesGreaterThan(); break;
			default: System.out.println("Scelta non valida."); break;
		}
	}
	
	private void insertAllData()  {
		// This format and logline originate from here:
        // http://stackoverflow.com/questions/20349184/java-parse-log-file
		String logformat = "%h %l %u %t \"%r\" %>s %b";
        
        Scanner input = new Scanner(System.in);
        Parser<LogRecord> parser = new HttpdLoglineParser<>(LogRecord.class, logformat);
        
        try {
            input = new Scanner(new File("access_log"));

            while (input.hasNextLine()) {                
                LogRecord record = new LogRecord();
                parser.parse(record, input.nextLine());
                //System.out.println("==================================================================================\n");
                crudOps.insertAll(record);
                //System.out.println(record.toString());
            }
            
            input.close();
            System.out.println("Elementi inseriti con successo all'intero del DB!");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        
	}
	
	private void showSelectedData(int choice) {
		List<Row> rows = null;
		
		switch(choice) {
			case 2: rows = crudOps.readAll(); break;
			case 3: rows = crudOps.getByResponseCode(); break;
			case 4: rows = crudOps.getByResponseCodeAndProtocolVersion(); break;
			case 5: rows = crudOps.getRequestUriSubstring(); break;
			case 6: rows = crudOps.getBytesReceivedGreaterThan(); break;
		}
		
        System.out.println("Lista degli elementi presenti nel DB:");
		for(Row row : rows) {
			System.out.println(row.toString());
		}
	}
	
	private void showGetURIWithReceivedBytesGreaterThan() {
		List<Row> rows = crudOps.getURIWithReceivedBytesGreaterThan();
	    System.out.println("Lista degli elementi presenti nel DB:");
		for(Row row : rows) {
			System.out.println(row.toString());
		}
	}
	
	private void showGetLimitedURIWithReceivedBytesGreaterThan() {
		List<Row> rows = crudOps.getLimitedURIWithReceivedBytesGreaterThan();
	    System.out.println("Lista degli elementi presenti nel DB:");
		for(Row row : rows) {
			System.out.println(row.toString());
		}
	}
	
	private void getCountURIWithReceivedBytesGreaterThan() {
		System.out.println(crudOps.getCountURIWithReceivedBytesGreaterThan().toString());
	}
}
