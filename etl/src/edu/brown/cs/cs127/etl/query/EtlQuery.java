package edu.brown.cs.cs127.etl.query;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.sqlite.util.StringUtils;

public class EtlQuery
{
	private Connection conn;

	public EtlQuery(String pathToDatabase) throws Exception{
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:" + pathToDatabase);

		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
	}
	
	public ResultSet query1(String[] args) throws SQLException{
		/**
		 * For some sample JDBC code, check out 
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT count(*) FROM airports"
		);
		return stat.executeQuery();
	}
	
	public ResultSet query2(String[] args) throws SQLException{
		/**
		 * For some sample JDBC code, check out 
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT count(*) FROM airlines"
		);
		return stat.executeQuery();
	}
	
	

	public ResultSet query3(String[] args) throws SQLException{
		PreparedStatement stat = conn.prepareStatement(
				"SELECT count(*) FROM flights"
			);
		return stat.executeQuery();
	}
	
	public ResultSet query4(String[] args) throws SQLException{
		PreparedStatement stat = conn.prepareStatement(
				 "SELECT * "
				 + "FROM "
				 + "(SELECT 'Carrier Delay' as Delay ,count(carrier_delay) as Count FROM flights WHERE carrier_delay > 0"
				 + " UNION "
				 + "SELECT 'Weather Delay' as Delay ,count(weather_delay) as Count FROM flights WHERE weather_delay > 0"
				 + " UNION "
				 + "SELECT 'Air Traffic Delay' as Delay ,count(air_traffic_delay) as Count FROM flights WHERE air_traffic_delay > 0"
				 + " UNION "
				 + "SELECT 'Security Delay' as Delay ,count(security_delay) as Count FROM flights WHERE security_delay > 0)"
				 + "ORDER BY Count DESC"
			);
		return stat.executeQuery();
	}
	
	public ResultSet query5(String[] args) throws SQLException{
		if(args.length < 5){
			System.out.println("too few arguments");
			return null;
		}
		//System.out.println("querying...");
		PreparedStatement stat = conn.prepareStatement(
					"SELECT  origin_airport_code , dest_airport_code , depart_date ||' '|| depart_time as departTime"
					+ " FROM flights"
					+ " WHERE airline_code = ? AND flight_num = ? AND depart_date = ?"
		);
		stat.setString(1, args[0]);
		stat.setInt(2, Integer.parseInt(args[1]));
		String date = String.format("%4d-%02d-%02d",Integer.valueOf(args[4]),Integer.valueOf(args[2]),Integer.valueOf(args[3]));
		stat.setString(3, date);
		return stat.executeQuery();
	}
	
	public ResultSet query6(String[] args) throws SQLException{
		if(args.length < 3){
			System.out.println("too few arguments");
			return null;
		}
		//System.out.println("querying...");
		PreparedStatement stat = conn.prepareStatement(
					"SELECT  l.airline_name, count(flight_id) as number"
					+ " FROM airlines as l "
					+ " LEFT OUTER JOIN "
					+ " (SELECT airline_code,flight_id"
					+ " FROM flights"
					+ " WHERE  depart_date = ?) as f"
					+ " ON l.airline_code = f.airline_code"
					+ " GROUP BY l.airline_name"
					+ " ORDER BY number DESC,airline_name"
		);
		String date = String.format("%4d-%02d-%02d",Integer.valueOf(args[2]),Integer.valueOf(args[0]),Integer.valueOf(args[1]));
		stat.setString(1, date);
		return stat.executeQuery();
	}
	
	public ResultSet query7(String[] args) throws SQLException{
		if(args.length < 4){
			System.out.println("too few arguments");
			return null;
		}
		//System.out.println("querying..."); 
		String str = "";
		
		String date = String.format("%4d-%02d-%02d",Integer.valueOf(args[2]),Integer.valueOf(args[0]),Integer.valueOf(args[1]));
		int count = args.length - 3;
		for(int i = 3;i < args.length;++i){
			//System.out.println(i);
			str += "?";
			if(i != args.length -1) str += ",";
		}
		 PreparedStatement stat = conn.prepareStatement("WITH a AS"
		    		+ " (SELECT *"
		    		+ " FROM airports as p"
		    		+ " LEFT OUTER JOIN"
		    		+ " flights as f "
		    		+ " ON p.airport_code = f.origin_airport_code"
		    		+ " WHERE airport_name in (" + str
		    		+ ") AND (depart_date = ? OR depart_date IS NULL)),"
		    		+ " b AS"
		    		+ " (SELECT *"
		    		+ " FROM airports as p"
		    		+ " LEFT OUTER JOIN"
		    		+ " flights as f "
		    		+ " ON p.airport_code = f.dest_airport_code"
		    		+ " WHERE airport_name in ("+ str
		    		+ ") AND (arrival_date = ? OR arrival_date IS NULL))"
		    		+ " SELECT  aa.airport_name, depart_num, arrival_num"
		    		+ " FROM"
		    		+ " (SELECT airport_name,count(flight_num) as depart_num"
		    		+ " FROM a"
		    		+ " GROUP BY airport_name)  as aa,"
		    		+ " (SELECT airport_name,count(flight_num) as arrival_num"
		    		+ " FROM b"
		    		+ " GROUP BY airport_name) as bb"
		    		+ " WHERE aa.airport_name = bb.airport_name ");
		 for(int i = 3;i < args.length;++i){
			 stat.setString(i-2, args[i]);
			 stat.setString(i-2+ count + 1, args[i]);
		 }
	    stat.setString(count+1, date);
	    stat.setString(2*count+2, date);
		return stat.executeQuery();
	}
	
	public ResultSet query8(String[] args) throws SQLException{
		if(args.length < 4){
			System.out.println("too few arguments");
			return null;
		}
		String airline_name = args[0];
		int flight_num = Integer.valueOf(args[1]);
		String lowerdate = String.format("%4d-%02d-%02d",Integer.valueOf(args[2].split("/")[2]),Integer.valueOf(args[2].split("/")[0]),Integer.valueOf(args[2].split("/")[1]));
		String upperdate = String.format("%4d-%02d-%02d",Integer.valueOf(args[3].split("/")[2]),Integer.valueOf(args[3].split("/")[0]),Integer.valueOf(args[3].split("/")[1]));
		//System.out.println(lowerdate);
		//System.out.println(upperdate);
		PreparedStatement stat = conn.prepareStatement(
					"WITH data AS"
					+ " (SELECT *"
					+ " FROM flights as f,airlines as l"
					+ " WHERE strftime('%s',f.depart_date) - strftime('%s',?)>= 0 AND strftime('%s',f.depart_date) - strftime('%s',?) <= 0"
					+ " AND f.airline_code = l.airline_code AND f.flight_num = ? AND l.airline_name = ?)"
					+ " SELECT *"
					+ " FROM"
					+ " (SELECT  count(flight_num) as total_num"
					+ " FROM data),"
					+ " (SELECT  count(cancelled) as cancelled_num"
					+ " FROM data"
					+ " WHERE cancelled = 1),"
					+ " (SELECT   count(flight_num) as depart_early"
					+ " FROM data"
					+ " WHERE cancelled = 0 AND depart_diff <= 0),"
					+ " (SELECT   count(flight_num) as depart_late"
					+ " FROM data"
					+ " WHERE cancelled = 0 AND depart_diff > 0),"
					+ " (SELECT   count(flight_num) as arrival_early"
					+ " FROM data"
					+ " WHERE cancelled = 0 AND arrival_diff <= 0),"
					+ " (SELECT   count(flight_num) as arrival_late"
					+ " FROM data"
					+ " WHERE cancelled = 0 AND arrival_diff > 0)"
		);
		
		stat.setString(1, lowerdate);
		stat.setString(2, upperdate);
		stat.setInt(3, flight_num);
		stat.setString(4, airline_name);
		return stat.executeQuery();
	}
	
	public ResultSet query9(String[] args) throws SQLException{
		if(args.length < 5){
			System.out.println("too few arguments");
			return null;
		}
		String depart_city = args[0];
		String depart_state = args[1];
		String arrival_city = args[2];
		String arrival_state = args[3];
		String lowerdate = String.format("%4d-%02d-%02d",Integer.valueOf(args[4].split("/")[2]),Integer.valueOf(args[4].split("/")[0]),Integer.valueOf(args[4].split("/")[1]));
		String upperdate = String.format("%4d-%02d-%02d 24:00",Integer.valueOf(args[4].split("/")[2]),Integer.valueOf(args[4].split("/")[0]),Integer.valueOf(args[4].split("/")[1]));
		//System.out.println(lowerdate);
		//System.out.println(upperdate);
		PreparedStatement stat = conn.prepareStatement(
					"WITH data AS"
					+ " (SELECT * "
					+ " FROM  flights as f,airports as p1,airports as p2"
					+ " WHERE  f.origin_airport_code = p1.airport_code AND  f.dest_airport_code = p2.airport_code "
					+ " AND p1.city = ? AND p1.state = ? AND p2.city = ? AND p2.state = ?)"
					+ " SELECT airline_code,flight_num,origin_airport_code,"
					+ " strftime('%H:%M', "
					+ " datetime((strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60), 'unixepoch')) as actual_depart,"
					+ " dest_airport_code,"
					+ " strftime('%H:%M', "
					+ " datetime((strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60), 'unixepoch')) as actual_arrival,"
					+ " (strftime('%s', arrival_date) + strftime('%s', arrival_time) + arrival_diff*60 - (strftime('%s', depart_date) + strftime('%s', depart_time) + depart_diff*60))/60 as duration"
					+ " FROM data "
					+ " WHERE  strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)<0"
					+ " AND strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)>=0"
					+ " AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)<0"
					+ " AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)>=0"
					+ " AND cancelled = 0"
					+ " ORDER BY duration,airline_code");

		stat.setString(1, depart_city);
		stat.setString(2, depart_state);
		stat.setString(3, arrival_city);
		stat.setString(4, arrival_state);
		stat.setString(5,upperdate);
		stat.setString(6,lowerdate);
		stat.setString(7,upperdate);
		stat.setString(8,lowerdate);
		return stat.executeQuery();
	}
	
	
	public ResultSet query10(String[] args) throws SQLException{
		if(args.length < 5){
			System.out.println("too few arguments");
			return null;
		}
		String depart_city = args[0];
		String depart_state = args[1];
		String arrival_city = args[2];
		String arrival_state = args[3];
		String lowerdate = String.format("%4d-%02d-%02d",Integer.valueOf(args[4].split("/")[2]),Integer.valueOf(args[4].split("/")[0]),Integer.valueOf(args[4].split("/")[1]));
		String upperdate = String.format("%4d-%02d-%02d 24:00",Integer.valueOf(args[4].split("/")[2]),Integer.valueOf(args[4].split("/")[0]),Integer.valueOf(args[4].split("/")[1]));
		//System.out.println(lowerdate);
		//System.out.println(upperdate);
		PreparedStatement stat = conn.prepareStatement(
					"WITH data AS"
					+ " (SELECT * "
					+ " FROM  flights as f"
					+ " WHERE  "
					+ " strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)<0"
					+ " AND strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)>=0"
					+ " AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)<0"
					+ " AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)>=0)"
					+ " SELECT d1.airline_code, d1.flight_num, "
					+ " d1.origin_airport_code,"
					+ " strftime('%H:%M', datetime((strftime('%s', d1.depart_date || ' ' || d1.depart_time)  + d1.depart_diff*60), 'unixepoch')) as actual_depart1,"
					+ " d1.dest_airport_code,"
					+ " strftime('%H:%M', datetime((strftime('%s', d1.arrival_date || ' ' || d1.arrival_time)  + d1.arrival_diff*60), 'unixepoch')) as actual_arrival1,"
					+ " d2.airline_code, d2.flight_num, "
					+ " d2.origin_airport_code,"
					+ " strftime('%H:%M', datetime((strftime('%s', d2.depart_date || ' ' || d2.depart_time)  + d2.depart_diff*60), 'unixepoch')) as actual_depart2,"
					+ " d2.dest_airport_code,"
					+ " strftime('%H:%M', datetime((strftime('%s', d2.arrival_date || ' ' || d2.arrival_time)  + d2.arrival_diff*60), 'unixepoch')) as actual_arrival2,"
					+ " (strftime('%s', d2.arrival_date) + strftime('%s', d2.arrival_time) + d2.arrival_diff*60 - (strftime('%s', d1.depart_date) + strftime('%s', d1.depart_time) + d1.depart_diff*60) )/60 as duration"
					+ " FROM data as d1,data as d2,airports as p1,airports as p2"
					+ " WHERE  d1.origin_airport_code = p1.airport_code AND d2.dest_airport_code = p2.airport_code "
					+ " AND d1.dest_airport_code = d2.origin_airport_code"
					+ " AND p1.city = ? AND p1.state = ? AND p2.city = ? AND p2.state = ?"
					+ " AND strftime('%s', d1.arrival_date || ' ' || d1.arrival_time)  +d1.arrival_diff*60 - (strftime('%s', d2.depart_date || ' ' || d2.depart_time)  +d2.depart_diff*60)<0"
					+ " AND d1.cancelled = 0  AND d2.cancelled = 0"
					+ " ORDER BY duration,d1.airline_code,d2.airline_code");

		
		stat.setString(1,upperdate);
		stat.setString(2,lowerdate);
		stat.setString(3,upperdate);
		stat.setString(4,lowerdate);
		stat.setString(5, depart_city);
		stat.setString(6, depart_state);
		stat.setString(7, arrival_city);
		stat.setString(8, arrival_state);
		return stat.executeQuery();
	}
	
	public ResultSet query11(String[] args) throws SQLException{
		if(args.length < 5){
			System.out.println("too few arguments");
			return null;
		}
		String depart_city = args[0];
		String depart_state = args[1];
		String arrival_city = args[2];
		String arrival_state = args[3];
		String lowerdate = String.format("%4d-%02d-%02d",Integer.valueOf(args[4].split("/")[2]),Integer.valueOf(args[4].split("/")[0]),Integer.valueOf(args[4].split("/")[1]));
		String upperdate = String.format("%4d-%02d-%02d 24:00",Integer.valueOf(args[4].split("/")[2]),Integer.valueOf(args[4].split("/")[0]),Integer.valueOf(args[4].split("/")[1]));
		//System.out.println(lowerdate);
		//System.out.println(upperdate);
		PreparedStatement stat = conn.prepareStatement(
					"WITH start_code AS"
					+ "	(SELECT airport_code"
					+ "	FROM airports as p"
					+ "	WHERE  p.city = ? AND p.state = ?),"
					+ "	end_code AS"
					+ "	(SELECT airport_code"
					+ "	FROM airports as p"
					+ "	WHERE  p.city = ? AND p.state = ?),"
					+ "	data1 AS"
					+ "	(SELECT * "
					+ "	FROM  flights as f"
					+ "	WHERE  "
					+ "	strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)<0"
					+ "	AND strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)>=0"
					+ "	AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)<0"
					+ "	AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)>=0"
					+ "	AND f.origin_airport_code IN start_code"
					+ "	AND f.cancelled = 0),"
					+ "	data2 AS"
					+ "	(SELECT * "
					+ "	FROM  flights as f"
					+ "	WHERE  "
					+ "	strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)<0"
					+ "	AND strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)>=0"
					+ "	AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)<0"
					+ "	AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)>=0"
					+ "	AND f.dest_airport_code NOT IN start_code"
					+ "	AND f.origin_airport_code NOT IN end_code"
					+ "	AND f.cancelled = 0),"
					+ "	data3 AS"
					+ "	(SELECT * "
					+ "	FROM  flights as f"
					+ "	WHERE  "
					+ "	strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)<0"
					+ "	AND strftime('%s', depart_date || ' ' || depart_time)  + depart_diff*60 - strftime('%s',?)>=0"
					+ "	AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)<0"
					+ "	AND strftime('%s', arrival_date || ' ' || arrival_time)  + arrival_diff*60 - strftime('%s',?)>=0"
					+ "	AND f.dest_airport_code IN end_code"
					+ "	AND f.cancelled = 0)"
					+ "	SELECT d1.airline_code, d1.flight_num, "
					+ "	d1.origin_airport_code,"
					+ "	strftime('%H:%M', datetime((strftime('%s', d1.depart_date || ' '|| d1.depart_time)  + d1.depart_diff*60), 'unixepoch')) as actual_depart1,"
					+ "	d1.dest_airport_code,"
					+ "	strftime('%H:%M', datetime((strftime('%s', d1.arrival_date || ' ' || d1.arrival_time)  + d1.arrival_diff*60), 'unixepoch')) as actual_arrival1,"
					+ "	d2.airline_code, d2.flight_num, "
					+ "	d2.origin_airport_code,"
					+ "	strftime('%H:%M', datetime((strftime('%s', d2.depart_date || ' ' || d2.depart_time)  + d2.depart_diff*60), 'unixepoch')) as actual_depart2,"
					+ "	d2.dest_airport_code,"
					+ "	strftime('%H:%M', datetime((strftime('%s', d2.arrival_date || ' ' || d2.arrival_time)  + d2.arrival_diff*60), 'unixepoch')) as actual_arrival2,"
					+ "	d3.airline_code, d3.flight_num, "
					+ "	d3.origin_airport_code,"
					+ "	strftime('%H:%M', datetime((strftime('%s', d3.depart_date || ' ' || d3.depart_time)  + d3.depart_diff*60), 'unixepoch')) as actual_depart3,"
					+ "	d3.dest_airport_code,"
					+ "	strftime('%H:%M', datetime((strftime('%s', d3.arrival_date || ' ' || d3.arrival_time)  + d3.arrival_diff*60), 'unixepoch')) as actual_arrival3,"
					+ "	(strftime('%s', d3.arrival_date) + strftime('%s', d3.arrival_time) + d3.arrival_diff*60 - (strftime('%s', d1.depart_date) + strftime('%s', d1.depart_time) + d1.depart_diff*60))/60 as duration"
					+ "	FROM data1 as d1,data2 as d2,data3 as d3"
					+ "	WHERE d1.dest_airport_code = d2.origin_airport_code and d2.dest_airport_code = d3.origin_airport_code"
					+ "	AND strftime('%s', d1.arrival_date || ' ' || d1.arrival_time)  +d1.arrival_diff*60 - (strftime('%s', d2.depart_date || ' ' || d2.depart_time)  +d2.depart_diff*60)<0"
					+ "	AND strftime('%s', d2.arrival_date || ' ' || d2.arrival_time)  +d2.arrival_diff*60 - (strftime('%s', d3.depart_date || ' ' || d3.depart_time)  +d3.depart_diff*60)<0"
					+ " ORDER BY duration,d1.airline_code,d2.airline_code,d3.airline_code");

		stat.setString(1, depart_city);
		stat.setString(2, depart_state);
		stat.setString(3, arrival_city);
		stat.setString(4, arrival_state);
		stat.setString(5,upperdate);
		stat.setString(6,lowerdate);
		stat.setString(7,upperdate);
		stat.setString(8,lowerdate);
		stat.setString(9,upperdate);
		stat.setString(10,lowerdate);
		stat.setString(11,upperdate);
		stat.setString(12,lowerdate);
		stat.setString(13,upperdate);
		stat.setString(14,lowerdate);
		stat.setString(15,upperdate);
		stat.setString(16,lowerdate);
		return stat.executeQuery();
	}
}
