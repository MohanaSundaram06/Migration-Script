package org.example;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;
public class Main {
    public static void main(String[] args)  {

        try {
            Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "Mohanasundaram@06");
            Statement mysqlStatement = mysqlConnection.createStatement();
            ResultSet resultSet = mysqlStatement.executeQuery("SELECT * FROM employee");

            MongoClient mongoClient = new MongoClient("localhost", 27017);
            MongoDatabase mongoDatabase = mongoClient.getDatabase("testMongo");
            MongoCollection<Document> employeeCollection = mongoDatabase.getCollection("employee");

            long startTime = System.currentTimeMillis();
            int totalRecords = 0;
            int migratedRecords = 0;

            while (resultSet.next()) {
                totalRecords++;
                try {
                    Document record = new Document();
                    record.append("id", resultSet.getInt("id"))
                            .append("name", resultSet.getString("name"))
                            .append("age", resultSet.getInt("age"));

                    employeeCollection.insertOne(record);
                    migratedRecords ++;

                } catch (Exception e) {
                   System.out.println("Failed to Insert Record " +e.getMessage());
                }
            }

            long endTime = System.currentTimeMillis();
            long migrationTime = endTime - startTime;

            System.out.println("Number of records to be migrated: " + totalRecords);
            System.out.println("Number of records migrated: " + migratedRecords );
            System.out.println("Number of failed records: " + (totalRecords - migratedRecords));
            System.out.println("Time Taken to complete migration: " + migrationTime);
        }
        catch (Exception e){
            System.out.println("Failed connection to database " + e.getMessage());
        }
    }
}