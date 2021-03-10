package com.migrationepms.data;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//import java.util.Date;

public class Epms {
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	
	private Connection connect1 = null;
	private Statement statement1 = null;
	//private PreparedStatement preparedStatement1 = null;
	private ResultSet resultSet1 = null;

	public ResultSet readDataBase() throws Exception {
        try {
            
            Class.forName("com.mysql.cj.jdbc.Driver");
            connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/ctc2data?"+ "user=root&password=password");

            statement = connect.createStatement();
            
            resultSet = statement
                    .executeQuery("select * from tblvisits");
            writeResultSet(resultSet);
            
            return resultSet;

        } catch (Exception e) {
            throw e;
        }

    }
	
	public void moveDataToNewEpms() throws Exception {
        try {
            
            Class.forName("com.mysql.cj.jdbc.Driver");
            connect1 = DriverManager
                    .getConnection("jdbc:mysql://localhost/epms?"+ "user=root&password=password");

            statement1 = connect1.createStatement();
            
            resultSet1 = readDataBase();
            while (resultSet1.next()) {
            	ResultSetMetaData rsmd = resultSet1.getMetaData();
            	int rowsNumber = rsmd.getColumnCount();
            	for (int z = 1; z <= rowsNumber; z++) {
                    preparedStatement = connect1
                            .prepareStatement("insert into tblvisits(`PatientID`,`VisitDate`,`VisiTtypeCode`,`NowPregnant`,`FamilyPlanningID`,"
                            		+ "`FunctionalStatusCode`,`WHOStage`,`TBStatusCode`,`IPTReasonCode`,`ARVStatusCode`,`ARVReasonCode`,`ARVCode`,"
                            		+ "`Notes`,`StaffID`,`UserNumber`,`TheTimeStamp`)\n" + 
                            		" values (?, ?, ?, ?, ? , ?, ?,?, ?, ?, ?, ? , ?, ?,?,?)");
                    
                    String patientId = resultSet1.getString(1);
                    Date visitdate = resultSet1.getDate(2);
                    String patientId1 = resultSet1.getString(3);
                    String patientId2 = resultSet1.getString(4);
                    String patientId3 = resultSet1.getString(5);
                    String patientId4 = resultSet1.getString(6);
                    int patientId5 = resultSet1.getInt(7);
                    String patientId6 = resultSet1.getString(8);
                    int patientId7 = resultSet1.getInt(9);
                    int patientId8 = resultSet1.getInt(10);
                    int patientId9 = resultSet1.getInt(11);
                    int patientId10 = resultSet1.getInt(12);
                    String patientId11 = resultSet1.getString(13);
                    int patientId12 = resultSet1.getInt(14);
                    int patientId13 = resultSet1.getInt(15);
                    Date patientId14 = resultSet1.getDate(16);
                    
                    preparedStatement.setString(1, patientId);
                    preparedStatement.setDate(2, visitdate);
                    preparedStatement.setString(3, patientId1);
                    preparedStatement.setString(4, patientId2);
                    preparedStatement.setString(5, patientId3);
                    preparedStatement.setString(6, patientId4);
                    preparedStatement.setInt(7, patientId5);
                    preparedStatement.setString(8, patientId6);
                    preparedStatement.setInt(9, patientId7);
                    preparedStatement.setInt(10, patientId8);
                    preparedStatement.setInt(11, patientId9);
                    preparedStatement.setInt(12, patientId10);
                    preparedStatement.setString(13, patientId11);
                    preparedStatement.setInt(14, patientId12);
                    preparedStatement.setInt(15, patientId13);
                    preparedStatement.setDate(16, patientId14);
                    
                    preparedStatement.executeUpdate();
                    
                }
                System.out.println("");
            }
            
            

        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

    }

    private void writeResultSet(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
        	ResultSetMetaData rsmd = resultSet.getMetaData();
        	int columnsNumber = rsmd.getColumnCount();
        	for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }

    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
                resultSet1.close();
            }

            if (statement != null) {
                statement.close();
                statement1.close();
            }

            if (connect != null) {
                connect.close();
                connect1.close();
            }
        } catch (Exception e) {

        }
    }

}