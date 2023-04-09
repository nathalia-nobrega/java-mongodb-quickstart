package com.mongodb.quickstart.crud;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.set;

public class BooksCrud {
    public static void main(String[] args) {
        connectToCluster();
    }

    private static void connectToCluster() {
        try (InputStream in = new FileInputStream("src/main/resources/application.properties")) {
            Properties props = new Properties();
            props.load(in);
            String connectionURI = props.getProperty("mongodb.uri");
            connectToDatabase(connectionURI);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void connectToDatabase(String connectionURI) {
        try (MongoClient client = MongoClients.create(connectionURI)) {
            MongoCollection<Document> books = client.getDatabase("Bookshop").getCollection("Books");
            System.out.println("------ Before updating ------\n");
            deleteDocuments(books);
            createDocuments(books);
            readDocuments(books);
            updateDocuments(books);

        }
    }

    private static void updateDocuments(MongoCollection<Document> books) {
        // Updating the number of pages of The Trial...
        Document first = books.find(eq("name", "The Trial")).first();
        Bson update = set("pages", 310);
        if (first != null) {
            books.updateOne(first, update);
        } else System.out.println("The query didn't find a result: book not present");

        // Adding a new field called isAvailable and assigning a value to it...
        List<Document> docs = books.find().into(new ArrayList<>());
        docs.forEach(document -> books.updateOne(document, set("isAvailable", new Random().nextBoolean())));

        // Adding a new field called yearOfRelease and assigning a value to it...
        docs.forEach(document -> books.updateMany(document, set("yearOfRelease", new Random().nextInt(1810, 1925))));

        // Adding a new field called distributors and assigning multiple values to it...
        List<String> distributors = List.of("ProsePlanet", "SwiftReads", "CoverConnect", "Inklinkers", "Papertrailers", "PublishingGrade", "Inkbounds");

        docs.forEach(document -> books.updateMany(document, set("distributors", distributors.subList(0, new Random().nextInt(distributors.size())).stream().limit(3).toList())));

        System.out.println("------ After updating ------\n");
        System.out.println("\nBooks that are available in the stock: ");
    }

    private static void deleteDocuments(MongoCollection<Document> books) {
        books.deleteMany(new Document());
    }

    private static void readDocuments(MongoCollection<Document> books) {
        System.out.println("Books that have over 300 pages: ");
        try (MongoCursor<Document> cursor = books.find(gte("pages", 300)).cursor()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().get("name"));
            }
        }

        System.out.println("\nBooks that have at least 200 pages: ");
        try (MongoCursor<Document> cursor = books.find(or(gte("pages", 200), eq("pages", 200))).cursor()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().get("name"));
            }
        }

        System.out.println("\nBooks that are available in the stock: ");
        try (MongoCursor<Document> cursor = books.find(eq("isAvailable", true)).cursor()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().get("name"));
            }
        }
    }

    private static void createDocuments(MongoCollection<Document> books) {
        List<Document> bookList = books.find().into(new ArrayList<>());
        bookList.addAll(List.of(
                new Document("name", "The Catcher In The Rye")
                        .append("pages", 200)
                        .append("author", "J.D Salinger"),
                new Document("name", "The Trial")
                        .append("pages", 305)
                        .append("author", "Franz Kafka"),
                new Document("name", "Notes from the Undergroud")
                        .append("pages", 191)
                        .append("author", "Fyodor Dostoevsky")));
        books.insertMany(bookList);
    }
}
