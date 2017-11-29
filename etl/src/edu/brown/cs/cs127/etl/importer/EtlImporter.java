package edu.brown.cs.cs127.etl.importer;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.com.bytecode.opencsv.CSVReader;
import edu.brown.cs.cs127.etl.model.Airlines;
import edu.brown.cs.cs127.etl.model.Airports;

public class EtlImporter
{
	/**
	 * You are only provided with a main method, but you may create as many
	 * new methods, other classes, etc as you want: just be sure that your
	 * application is runnable using the correct shell scripts.
	 */
	
	private static Connection conn;
	private static CSVReader reader;
	private static Map<String,Airports> airports;
	private static Map<String,Airlines> airlines;
	private static Map<Pattern,SimpleDateFormat> format = new HashMap<Pattern,SimpleDateFormat>();
	
	public static void main(String[] args) throws Exception
	{
		long start_time = System.currentTimeMillis();
		System.out.println(start_time);
		if (args.length != 4)
		{
			System.err.println("This application requires exactly four parameters: " +
					"the path to the airports CSV, the path to the airlines CSV, " +
					"the path to the flights CSV, and the full path where you would " +
					"like the new SQLite database to be written to.");
			System.exit(1);
		}

		String AIRPORTS_FILE = args[0];
		String AIRLINES_FILE = args[1];
		String FLIGHTS_FILE = args[2];
		String DB_FILE = args[3];
		
		airports = new HashMap<String,Airports>();
		airlines = new HashMap<String,Airlines>();
		format.put(Pattern.compile("\\d{4}-\\d{2}-\\d{2}"), new SimpleDateFormat("yyyy-MM-dd"));
		format.put(Pattern.compile("\\d{4}/\\d{2}/\\d{2}"), new SimpleDateFormat("yyyy/MM/dd"));
		format.put(Pattern.compile("\\d{2}-\\d{2}-\\d{4}"), new SimpleDateFormat("MM-dd-yyyy"));
		format.put(Pattern.compile("\\d{2}/\\d{2}/\\d{4}"), new SimpleDateFormat("MM/dd/yyyy"));
		
		// INITIALIZE THE CONNECTION
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);

			ReadWrite_airlines(AIRLINES_FILE);
			ReadWrite_airports(AIRPORTS_FILE);
			ReadWrite_flights(FLIGHTS_FILE);
		

			long end_time = System.currentTimeMillis();
			System.out.println(end_time);
			System.out.println((end_time - start_time)/1000);
		
		
		/*
		 * READING DATA FROM CSV FILES
		 * Source: http://opencsv.sourceforge.net/#how-to-read
		 * 
		 * If you want to use an Iterator style pattern, you might do something like this: 
		 * 
		 *	CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
		 *	String [] nextLine;
		 *	while ((nextLine = reader.readNext()) != null) {
		 *		// nextLine[] is an array of values from the line
		 *		System.out.println(nextLine[0] + nextLine[1] + "etc...");
		 * }
		 * 
		 * Or, if you might just want to slurp the whole lot into a List, just call readAll()... 
		 * 
		 *	CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
		 *	List myEntries = reader.readAll();
		 */

		/*
		 * Below are some snippets of JDBC code that may prove useful
		 * 
		 * For more sample JDBC code, check out 
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 * 
		 * ---
		 * 
		 *	// INITIALIZE THE CONNECTION
		 *	Class.forName("org.sqlite.JDBC");
		 *	Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);
		 *
		 * ---
		 *
		 *	// ENABLE FOREIGN KEY CONSTRAINT CHECKING
		 *	Statement stat = conn.createStatement();
		 *	stat.executeUpdate("PRAGMA foreign_keys = ON;");
		 *
		 *	// Speed up INSERTs
		 *	stat.executeUpdate("PRAGMA synchronous = OFF;");
		 *	stat.executeUpdate("PRAGMA journal_mode = MEMORY;");
		 *
		 * ---
		 * 
		 *	// You can execute DELETE statements before importing if you want to be
		 *	// able to overwrite an existing database.
		 *	stat.executeUpdate("DROP TABLE IF EXISTS table;");
		 *
		 * ---
		 * 
		 * 	// Normally the database throws an exception when constraints are enforced
		 *	// and an INSERT statement that violates a constraint is executed. This is true
		 *	// even when doing a batch insert (multiple rows in one statement), causing all
		 *	// rows in the statement to not be inserted into the database.
		 *
		 *	// As a result, if you want the efficiency gains of using batch inserts, you need to be smart:
		 *	// You need to make sure your application enforces foreign key constraints before the insert ever happens.
		 * 	PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO table (col1, col2) VALUES (?, ?)");
		 * 	List<String[]> rowInfo = getTableRows();
		 *  for (String[] curRow : rowInfo)
		 *  {
		 *  	prep.setString(1, curRow[0]);
		 *  	prep.setInt(2, curRow[1]);
		 *  	prep.addBatch();
		 *  }
		 *  
		 *  // We temporarily disable auto-commit, allowing the batch to be sent
		 *  // as one single transaction. Then we re-enable it, executing the batch.
		 *  conn.setAutoCommit(false);
		 *  prep.executeBatch();
		 *  conn.setAutoCommit(true);
		 * 	
		 */
		
		
		
	}


	private static void ReadWrite_flights(String filename) throws SQLException, NumberFormatException, IOException, ParseException {
		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
		stat.executeUpdate("PRAGMA synchronous = OFF;");
		stat.executeUpdate("PRAGMA journal_mode = MEMORY;");		
		stat.executeUpdate("DROP TABLE IF EXISTS flights;");
	    String flights_cr = "CREATE TABLE flights ("
	    		+ "flight_id smallint not NULL, "
	    		+ "airline_code VARCHAR(7) not NULL, "
	    		+ "flight_num VARCHAR(4) not NULL, "
	    		+ "origin_airport_code CHAR(3), "
	    		+ "dest_airport_code CHAR(3), "
	    		+ "depart_date CHAR(10) CHECK (depart_date <= arrival_date), "
	    		+ "depart_time CHAR(5) CHECK ( (strftime('%s', depart_date) + strftime('%s',depart_time) + depart_diff*60) < (strftime('%s', arrival_date) + strftime('%s',arrival_time) + arrival_diff*60)), "
	    		+ "depart_diff smallint, "
	    		+ "arrival_date CHAR(10), "
	    		+ "arrival_time CHAR(5) CHECK ( (strftime('%s', depart_date) + strftime('%s',depart_time)) < (strftime('%s', arrival_date) + strftime('%s',arrival_time))), "
	    		+ "arrival_diff smallint, "
	    		+ "cancelled smallint CHECK (cancelled = 0 or cancelled = 1), "
	    		+ "carrier_delay smallint CHECK (carrier_delay >= 0), "
	    		+ "weather_delay smallint CHECK (weather_delay >= 0), "
	    		+ "air_traffic_delay smallint CHECK (air_traffic_delay >= 0), "
	    		+ "security_delay smallint CHECK (security_delay >= 0), "
	    		+ "FOREIGN KEY (origin_airport_code) REFERENCES airports(airport_code), "
	    		+ "FOREIGN KEY (dest_airport_code) REFERENCES airports(airport_code), "
	    		+ "FOREIGN KEY (airline_code) REFERENCES airlines(airline_code), "
	    		+ "PRIMARY KEY (flight_id))";
	        
	    stat.executeUpdate(flights_cr);
	    reader = new CSVReader(new FileReader(filename),',');
	    int count = 1;
	    String [] line;
	    PreparedStatement prepinsert = null;
	    PreparedStatement prepupdate = null;
	    
	    prepinsert = conn.prepareStatement("INSERT OR IGNORE INTO flights ("
	    		+ "flight_id, "            //1
	    		+ "airline_code, "         //2 
	    		+ "flight_num, "           //3
 	    		+ "origin_airport_code, "  //4
	    		+ "dest_airport_code, "    //5
	    		+ "depart_date, "          //6
	    		+ "depart_time, "          //7
	    		+ "depart_diff, "          //8
				+" arrival_date, "         //9
				+ "arrival_time, "         //10
				+ "arrival_diff, "         //11
				+ "cancelled, "            //12
				+ "carrier_delay, "        //13
				+ "weather_delay, "        //14
				+ "air_traffic_delay, "    //15
				+ "security_delay) "       //16
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
	    
	    prepupdate = conn.prepareStatement("UPDATE airports SET city= ?, state= ? WHERE airport_code= ?; ");
	    while ((line = reader.readNext()) != null) {
	    	int flight_id = count++;
			String airline_code = line[0];
			String flight_num = line[1];
			
			String origin_airport_code = line[2];
			String origin_airport_city = line[3];
			String origin_airport_state = line[4];
			
			String dest_airport_code = line[5];
			String dest_airport_city = line[6];
			String dest_airport_state = line[7];
			
			String depart_date = parseDate(line[8]);
			String depart_time = parseTime(line[9]);
			//System.out.println(depart_time);
			int depart_diff = Integer.valueOf(line[10]);
			String arrival_date = parseDate(line[11]);
			String arrival_time = parseTime(line[12]);
			//System.out.println(arrival_time);
			int arrival_diff = Integer.valueOf(line[13]);
			int cancelled = line[14].equals("0") ? 0 : 1;
			int carrier_delay = Integer.valueOf(line[15]);
			int weather_delay = Integer.valueOf(line[16]);
			int air_traffic_delay = Integer.valueOf(line[17]);
			int security_delay = Integer.valueOf(line[18]);
			
			if(!airports.containsKey(origin_airport_code) || !airports.containsKey(dest_airport_code) || !airlines.containsKey(airline_code)){
				continue;
			}
			
			Airports origin_port = airports.get(origin_airport_code);
			if(origin_port.getCity() == ""|| origin_port.getState() == ""){
				origin_port.setCity(origin_airport_city);
				airports.put(origin_airport_code, origin_port);
				origin_port.setState(origin_airport_state);
				airports.put(origin_airport_code, origin_port);
				
				//update 
				prepupdate.setString(1, origin_port.getCity());
				prepupdate.setString(2, origin_port.getState());
				prepupdate.setString(3, origin_airport_code);
				prepupdate.addBatch();
			}
	
			Airports dest_port = airports.get(dest_airport_code);
			if(dest_port.getCity() == "" || dest_port.getState() == ""){
				dest_port.setCity(dest_airport_city);
				airports.put(dest_airport_code, dest_port);
				dest_port.setState(dest_airport_state);
				airports.put(dest_airport_code, dest_port);
				
				//update
				prepupdate.setString(1, dest_port.getCity());
				prepupdate.setString(2, dest_port.getState());
				prepupdate.setString(3, dest_port.getAirport_code());
				prepupdate.addBatch();
			}
		
		       
			prepinsert.setInt(1, flight_id);
			prepinsert.setString(2, airline_code);
			prepinsert.setString(3, flight_num);
			prepinsert.setString(4, origin_airport_code);
			prepinsert.setString(5, dest_airport_code);
			prepinsert.setString(6, depart_date);
			prepinsert.setString(7, depart_time);
			prepinsert.setInt(8, depart_diff);
			prepinsert.setString(9, arrival_date);
			prepinsert.setString(10, arrival_time);
			prepinsert.setInt(11,arrival_diff);
			prepinsert.setInt(12, cancelled);
			prepinsert.setInt(13, carrier_delay);
			prepinsert.setInt(14, weather_delay);
			prepinsert.setInt(15, air_traffic_delay);
			prepinsert.setInt(16, security_delay);
			
			prepinsert.addBatch();
//		     if(count == 100){
//		        conn.setAutoCommit(false);
//		        prepinsert.executeBatch();
//		        conn.setAutoCommit(true);
//		        prepinsert.close();
//		        return;
//		     }
			
		}
	    conn.setAutoCommit(false);
	    prepinsert.executeBatch();
	    prepupdate.executeBatch();
        conn.setAutoCommit(true);
        prepinsert.close();
        prepupdate.close();
	    System.out.printf("insert flights:%d\n",count);
	}
	
	private static void ReadWrite_airlines(String filename) throws Exception {
		Statement stat = conn.createStatement();
		//stat.executeUpdate("PRAGMA foreign_keys = ON;");
		stat.executeUpdate("PRAGMA synchronous = OFF;");
		stat.executeUpdate("PRAGMA journal_mode = MEMORY;");		
		stat.executeUpdate("DROP TABLE IF EXISTS airlines;");
	    String airports_cr = "CREATE TABLE airlines ("
	    		+ "airline_code VARCHAR(7) not NULL, "
	    		+ "airline_name VARCHAR(255), "
	    		+ "PRIMARY KEY ( airline_code ))";
	    stat.executeUpdate(airports_cr);
	    
		reader = new CSVReader(new FileReader(filename),',');
	    String [] line;
	    PreparedStatement prep = null;
	    int count = 0;
	    prep = conn.prepareStatement("INSERT INTO airlines (airline_code, airline_name) VALUES (?, ?)");
	    while ((line = reader.readNext()) != null) {
	    	count++;
	    	Airlines oneline = new Airlines(line[0],line[1]);
	    	airlines.put(line[0], oneline);
	    	prep.setString(1, line[0]);
	    	prep.setString(2, line[1]);
	    	prep.addBatch();
//	    	if(count == 500){
//		        conn.setAutoCommit(false);
//		        prep.executeBatch();
//		        conn.setAutoCommit(true);
//		        
//		     }
	    }
	    conn.setAutoCommit(false);
	    prep.executeBatch();
	    conn.setAutoCommit(true);
	    prep.close();
	    System.out.printf("insert airlines:%d\n",count);
	    
	}

	private static void ReadWrite_airports(String filename) throws Exception {
		Statement stat = conn.createStatement();
		//stat.executeUpdate("PRAGMA foreign_keys = ON;");
		stat.executeUpdate("PRAGMA synchronous = OFF;");
		stat.executeUpdate("PRAGMA journal_mode = MEMORY;");		
		stat.executeUpdate("DROP TABLE IF EXISTS airports;");
	    String airports_cr = "CREATE TABLE airports "
	    		+ "(airport_code CHAR(3) not NULL, "
	    		+ "airport_name VARCHAR(255), "
	    		+ "city VARCHAR(255), "
	    		+ "state VARCHAR(255), "
	    		+ "PRIMARY KEY ( airport_code ))";
	    stat.executeUpdate(airports_cr);
	    
		reader = new CSVReader(new FileReader(filename),',');
	    String [] line;
	    PreparedStatement prep = null;
	    int count = 0;
	    prep = conn.prepareStatement("INSERT INTO airports (airport_code, airport_name, city, state) VALUES (?, ?, ?, ?)");
	    while ((line = reader.readNext()) != null) {
	    	count++;
	    	Airports oneport = new Airports(line[0],line[1],"","");
	    	airports.put(line[0], oneport);
	    	prep.setString(1, line[0]);
	    	prep.setString(2, line[1]);
	    	prep.setString(3, null);
	    	prep.setString(4, null);
	    	prep.addBatch();
//	    	if(count == 1000){
//		        conn.setAutoCommit(false);
//		        prep.executeBatch();
//		        conn.setAutoCommit(true);
//		        
//		     }
	    }
	    conn.setAutoCommit(false);
	    prep.executeBatch();
	    conn.setAutoCommit(true);
	    prep.close();
	    System.out.printf("insert airports:%d\n",count);
	}
	
	private static String parseTime(String string) {
		int hour;
		int minute;
		hour = Integer.parseInt(string.substring(0, 2));
		minute = Integer.parseInt(string.substring(3, 5));
		if(string.contains("AM") || string.contains("PM")){
			if(hour == 12) hour = 0;
			if(string.contains("PM")){
				hour += 12;
			}
		}
		return String.format("%02d:%02d", hour,minute);
	}

	private static String parseDate(String string) throws ParseException {
		for(Map.Entry<Pattern,SimpleDateFormat> entry : format.entrySet()){
			 Matcher m = entry.getKey().matcher(string);
			if(m.matches()){
				Date date = entry.getValue().parse(string);
				//System.out.printf("year:%d,month:%d,day:%d\n",date.getYear()+1900,date.getMonth()+1,date.getDate());
				String res = String.format("%4d-%02d-%02d", date.getYear()+1900,date.getMonth()+1,date.getDate());
				return res;
			}
		}
		throw new IllegalArgumentException("no this date pattern");
	}

	

	
}
