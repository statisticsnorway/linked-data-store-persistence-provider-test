package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistenceBridge;
import no.ssb.lds.api.persistence.reactivex.RxPersistenceBridge;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.core.persistence.test.PersistenceIntegrationTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

import static java.util.Optional.ofNullable;

public class MemoryPersistenceIntegrationTest extends PersistenceIntegrationTest {

    public MemoryPersistenceIntegrationTest() {
        super("lds-provider-memory-integration-test");
    }

    @BeforeClass
    public void setup() {
        JsonPersistence memoryJsonPersistence = new MemoryInitializer().initialize(namespace,
                Map.of("persistence.mem.wait.min", "0",
                        "persistence.mem.wait.max", "0"),
                Set.of("Person", "Address", "FunkyLongAddress"));
        Persistence memoryPersistence = memoryJsonPersistence.getPersistence();
        this.persistence = new RxJsonPersistenceBridge(new RxPersistenceBridge(memoryPersistence),8192);
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
