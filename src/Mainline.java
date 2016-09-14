import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Mainline {

	static Connection conn = null;
	static Statement stat = null;
	static ResultSet rs = null;

	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {

		 // Add George Washington
		 updateDB("INSERT student (id,first_name,last_name,gpa,sat,major_id) "
		 		+ "values (999,'George','Washington',4.0,1600,NULL)");
		 // Full GW's data
		 selectStudent(999);
		 // Update GW's data
		 updateDB("UPDATE student SET gpa = 3.5, sat = 1450, major_id = 1 where id = 999");
		 selectStudent(999);
		 // Delete GW's data
		 updateDB("DELETE FROM student where last_name = 'Washington' AND sat = 1450");
		 selectStudent(999);

		 //Create list of queries needed to recover student table
		 ArrayList<String> studentRecoveryQueries = backupStudent();
		
		// Use list to write an sql file
		//writeListToFile(studentRecoveryQueries);
		
		// Recover student table using file
		//recoverStudentTable();
		
	}

	//Method to update database data
	static void updateDB(String query) throws SQLException {

		try {
			Properties props = new Properties();
			props.load(new FileInputStream("db.properties"));
			String username = props.getProperty("user");
			String password = props.getProperty("password");
			String url = props.getProperty("dburl");

			conn = DriverManager.getConnection(url, username, password);
			stat = conn.createStatement();

			int rowsAffected = stat.executeUpdate(query);
			System.out.println(rowsAffected + " row added/updated");

		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (conn != null)
				conn.close();
			if (stat != null)
				stat.close();

		}
	}

	//Method to retrieve a single student's info with the paramete of an id
	static void selectStudent(int studentId) throws SQLException {

		try {
			Properties props = new Properties();
			props.load(new FileInputStream("db.properties"));
			String username = props.getProperty("user");
			String password = props.getProperty("password");
			String url = props.getProperty("dburl");

			conn = DriverManager.getConnection(url, username, password);
			stat = conn.createStatement();

			rs = stat.executeQuery("SELECT * FROM student where id = " + studentId);

			System.out.println("+-----+------------+------------+--------+------+----------+");
			System.out.println("| ID  | First Name | Last Name  | GPA    | SAT  | Major ID |");
			System.out.println("+-----+------------+------------+--------+------+----------+");

			if (!rs.next()) {
				System.out.println("| No student record found with ID " + studentId + "                      |");
			}
			rs.beforeFirst();
			while (rs.next()) {
				System.out.print("| " + rs.getString("id") + " ");
				System.out.print("| " + rs.getString("first_name") + "     ");
				System.out.print("| " + rs.getString("last_name") + " ");
				System.out.print("| " + rs.getFloat("gpa") + "   ");
				System.out.print("| " + rs.getInt("sat") + "  ");
				System.out.println("| " + rs.getInt("major_id") + "        |");
			}
			System.out.println("+-----+------------+------------+--------+------+----------+");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (conn != null)
				conn.close();
			if (stat != null)
				stat.close();
			if (rs != null)
				rs.close();
		}

	}

	//Recovers a table given an array list of insert queries
	static void recoverTable(ArrayList<String> queries) throws SQLException {
		try {
			Properties props = new Properties();
			props.load(new FileInputStream("db.properties"));
			String username = props.getProperty("user");
			String password = props.getProperty("password");
			String url = props.getProperty("dburl");

			conn = DriverManager.getConnection(url, username, password);
			stat = conn.createStatement();

			int rowsAffected = 0;

			for (String query : queries) {
				int result = stat.executeUpdate(query);
				if (result == 1)
					rowsAffected++;
			}
			System.out.println(rowsAffected + " row(s) added/updated");

		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (conn != null)
				conn.close();
			if (stat != null)
				stat.close();

		}
	}

	//Writes an ArrayList of Strings to an sql file
	static void writeListToFile(ArrayList<String> list) {

		for (String query : list) {
			try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("student_recovery_queries.sql", true)))) {
				out.println(query + ";");
			} catch (IOException e) {
				System.out.println(e);
			}
		}


	}
	
	static void recoverStudentTable() throws FileNotFoundException, IOException, SQLException {
		
		try (BufferedReader in = new BufferedReader(new FileReader("student_recovery_queries.sql"))) {
			String line;
			
			while( (line = in.readLine()) != null) {
				updateDB(line);
			}
			
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	//Backup the student table to an array list
	static ArrayList<String> backupStudent() throws SQLException {

		ArrayList<String> insertQueries = new ArrayList<String>();

		try {
			Properties props = new Properties();
			props.load(new FileInputStream("db.properties"));
			String username = props.getProperty("user");
			String password = props.getProperty("password");
			String url = props.getProperty("dburl");
			conn = DriverManager.getConnection(url, username, password);
			stat = conn.createStatement();
			rs = stat.executeQuery("SELECT * FROM student");

			while (rs.next()) {
				insertQueries.add("INSERT student (id,first_name,last_name,gpa,sat,major_id) values ("
						+ rs.getInt("id") + "," + "'" + rs.getString("first_name") + "'" + "," + "'"
						+ rs.getString("last_name") + "'" + "," + rs.getDouble("gpa") + "," + rs.getInt("sat") + ","
						+ rs.getInt("major_id") + ")");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (conn != null)
				conn.close();
			if (stat != null)
				stat.close();
			if (rs != null)
				rs.close();
		}

		return insertQueries;
	}
	
//	// BACKUP TABLE GIVEN TABLE NAME (incomplete)
//	public ArrayList<String> backup(String table) throws SQLException {
//		
//		ArrayList<String> insertQueries = new ArrayList<String>();
//		
//		try {
//		    Properties props = new Properties();
//		    props.load(new FileInputStream("db.properties"));
//		    String username = props.getProperty("user");
//		    String password = props.getProperty("password");
//		    String url = props.getProperty("dburl");
//		    conn = DriverManager.getConnection(url, username, password);
//		    stat = conn.createStatement();
//		    rs = stat.executeQuery("SELECT * FROM " + table);
//		
//		    ResultSetMetaData rsmd = rs.getMetaData();
//		    HashMap<String, Integer> typeMap =  new HashMap<>();
//		    
//		    int columnCount = rsmd.getColumnCount();
//		    
//		    for (int i = 1; i <= columnCount; i++) {
//		    	
//		    	if(rsmd.getColumnTypeName(i) == "VARCHAR") {
//		    		typeMap.put(rsmd.getColumnName(i),1);
//		    	} else if(rsmd.getColumnTypeName(i) == "DATE") {
//		    		typeMap.put(rsmd.getColumnName(i),1);
//		    	} else if (rsmd.getColumnTypeName(i) == "INT") {
//		    		typeMap.put(rsmd.getColumnName(i), 0);
//		    	} else if (rsmd.getColumnTypeName(i) == "DECIMAL") {
//		    		typeMap.put(rsmd.getColumnName(i), 0);
//		    	}
//		    	
//		    	
//		    }
//		    
//		    
//
//		    while (rs.next()) {
//		    }
//	    } catch (SQLException e) {
//		    e.printStackTrace();
//	     } catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//		    if (conn != null)
//			conn.close();
//		    if (stat != null)
//			stat.close();
//		    if (rs != null)
//			rs.close();
//	    }
//		
//		return insertQueries;
//	}

}
