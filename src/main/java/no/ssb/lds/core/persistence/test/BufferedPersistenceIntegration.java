package no.ssb.lds.core.persistence.test;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.api.persistence.buffered.DocumentKey;
import no.ssb.lds.api.persistence.buffered.FlattenedDocument;
import no.ssb.lds.api.persistence.buffered.FlattenedDocumentIterator;
import no.ssb.lds.api.persistence.buffered.FlattenedDocumentLeafNode;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.persistence.streaming.Persistence;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class BufferedPersistenceIntegration {

    protected final String namespace;
    protected final int capacity;

    protected DefaultBufferedPersistence persistence;
    protected Persistence streaming;

    protected BufferedPersistenceIntegration(String namespace, int capacity) {
        this.namespace = namespace;
        this.capacity = capacity;
    }

    @Test
    public void thatDeleteAllVersionsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            FlattenedDocument input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
            persistence.createOrOverwrite(transaction, input0).join();
            FlattenedDocument input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
            persistence.createOrOverwrite(transaction, input1).join();
            FlattenedDocument input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
            persistence.createOrOverwrite(transaction, input2).join();
            Iterator<FlattenedDocument> iteratorWithDocuments = persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join();
            assertEquals(size(iteratorWithDocuments), 3);

            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            Iterator<FlattenedDocument> iterator = persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join();

            assertEquals(size(iterator), 0);
        }
    }

    int size(Iterator<?> iterator) {
        int i = 0;
        while (iterator.hasNext()) {
            iterator.next();
            i++;
        }
        return i;
    }

    @Test
    public void thatBasicCreateThenReadWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            FlattenedDocument input = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
            CompletableFuture<Void> completableFuture = persistence.createOrOverwrite(transaction, input);
            completableFuture.join();
            CompletableFuture<FlattenedDocumentIterator> completableDocumentIterator = persistence.read(transaction, oct18, namespace, "Person", "john");
            FlattenedDocumentIterator flattenedDocumentIterator = completableDocumentIterator.join();
            assertTrue(flattenedDocumentIterator.hasNext());
            FlattenedDocument output = flattenedDocumentIterator.next();
            assertNotNull(output);
            assertFalse(output == input);
            assertEquals(output, input);
        }
    }

    @Test
    public void thatBasicTimeBasedVersioningWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            FlattenedDocument input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
            persistence.createOrOverwrite(transaction, input0).join();
            FlattenedDocument input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
            persistence.createOrOverwrite(transaction, input1).join();
            FlattenedDocument input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
            persistence.createOrOverwrite(transaction, input2).join();
            Iterator<FlattenedDocument> iterator = persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join();
            Set<DocumentKey> actual = new LinkedHashSet<>();
            assertTrue(iterator.hasNext());
            actual.add(iterator.next().key());
            assertTrue(iterator.hasNext());
            actual.add(iterator.next().key());
            assertTrue(iterator.hasNext());
            actual.add(iterator.next().key());
            assertFalse(iterator.hasNext());
            assertEquals(actual, Set.of(input0.key(), input1.key(), input2.key()));
        }
    }

    @Test
    public void thatDeleteMarkerWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb1663 = ZonedDateTime.of(1663, 2, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));

            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664)).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join()), 3);

            persistence.markDeleted(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join()), 4);

            persistence.delete(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join()), 3);

            persistence.markDeleted(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join()), 4);
        }
    }

    @Test
    public void thatReadVersionsInRangeWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

            assertEquals(size(persistence.readVersions(transaction, feb10, sep18, namespace, "Person", "john", null, 100).join()), 2);
        }
    }


    @Test
    public void thatReadAllVersionsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Person", "john", null, 100).join()), 3);
        }
    }


    @Test
    public void thatFindAllWithPathAndValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
            persistence.deleteAllVersions(transaction, namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

            Iterator<FlattenedDocument> iterator = persistence.find(transaction, sep18, namespace, "Person", "lastname", "Smith", null, 100).join();

            FlattenedDocument person1 = iterator.next();
            FlattenedDocument person2 = iterator.next();
            assertFalse(iterator.hasNext());

            if (person1.contains("firstname", "Jane")) {
                assertTrue(person2.contains("firstname", "James"));
            } else {
                assertTrue(person1.contains("firstname", "James"));
                assertTrue(person2.contains("firstname", "Jane"));
            }
        }
    }


    @Test
    public void thatFindAllWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            // TODO Consider support for deleting entire entity in one operation...?
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
            persistence.deleteAllVersions(transaction, namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime dec11 = ZonedDateTime.of(2011, 12, 4, 16, 46, 23, (int) TimeUnit.MILLISECONDS.toNanos(304), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

            Iterator<FlattenedDocument> iterator = persistence.findAll(transaction, dec11, namespace, "Person", null, 100).join();

            FlattenedDocument person1 = iterator.next();
            FlattenedDocument person2 = iterator.next();
            assertFalse(iterator.hasNext());

            if (person1.contains("firstname", "Jane")) {
                assertTrue(person2.contains("firstname", "John"));
            } else {
                assertTrue(person1.contains("firstname", "John"));
                assertTrue(person2.contains("firstname", "Jane"));
            }
        }
    }

    @Test
    public void thatBigValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));

            String bigString = "12345678901234567890";
            for (int i = 0; i < 12; i++) {
                bigString = bigString + "_" + bigString;
            }

            // Creating funky long address
            persistence.createOrOverwrite(transaction, toDocument(namespace, "FunkyLongAddress", "newyork", createAddress(bigString, "NY", "USA"), oct18)).join();

            // Finding funky long address by city
            int findSize = size(persistence.find(transaction, now, namespace, "FunkyLongAddress", "city", bigString, null, 100).join());
            assertEquals(findSize, 1);

            // Finding funky long address by city (with non-matching value)
            int findExpectNoMatchSize = size(persistence.find(transaction, now, namespace, "FunkyLongAddress", "city", bigString + "1", null, 100).join());
            assertEquals(findExpectNoMatchSize, 0);

            // Deleting funky long address
            persistence.delete(transaction, namespace, "FunkyLongAddress", "newyork", oct18, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
        }
    }

    protected static JSONObject createPerson(String firstname, String lastname) {
        JSONObject person = new JSONObject();
        person.put("firstname", firstname);
        person.put("lastname", lastname);
        return person;
    }

    protected static JSONObject createAddress(String city, String state, String country) {
        JSONObject address = new JSONObject();
        address.put("city", city);
        address.put("state", state);
        address.put("country", country);
        return address;
    }

    protected FlattenedDocument toDocument(String namespace, String entity, String id, JSONObject json, ZonedDateTime timestamp) {
        DocumentKey key = new DocumentKey(namespace, entity, id, timestamp);
        Map<String, FlattenedDocumentLeafNode> leafNodeByPath = new TreeMap<>();
        addFragments(key, "$.", json, leafNodeByPath);
        return new FlattenedDocument(key, leafNodeByPath, false);
    }

    protected void addFragments(DocumentKey key, String pathPrefix, JSONObject json, Map<String, FlattenedDocumentLeafNode> leafNodeByPath) {
        for (Map.Entry<String, Object> entry : json.toMap().entrySet()) {
            String path = entry.getKey();
            Object untypedValue = entry.getValue();
            if (JSONObject.NULL.equals(untypedValue)) {
            } else if (untypedValue instanceof JSONObject) {
                addFragments(key, pathPrefix + "." + path, (JSONObject) untypedValue, leafNodeByPath);
            } else if (untypedValue instanceof JSONArray) {
                throw new UnsupportedOperationException("JSONArray");
            } else if (untypedValue instanceof String) {
                String value = (String) untypedValue;
                leafNodeByPath.put(path, new FlattenedDocumentLeafNode(key, path, FragmentType.STRING, value, capacity));
            } else if (untypedValue instanceof Number) {
                throw new UnsupportedOperationException("Number");
            } else if (untypedValue instanceof Boolean) {
                throw new UnsupportedOperationException("Boolean");
            }
        }
    }
}
