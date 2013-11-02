import org.msgpack.annotation.Message;


import datomic.Entity;
import datomic.Connection;
import datomic.Database;



public class Datomic {


    @Message //annotation
    public static class TestMessage
    {
        public String name;
        public double version;
    }


}
