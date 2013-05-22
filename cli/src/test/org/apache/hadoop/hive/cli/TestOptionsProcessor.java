package org.apache.hadoop.hive.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

public class TestOptionsProcessor {

  @Test
  public void test1() {
    OptionsProcessor processor = new OptionsProcessor();
    System.clearProperty("hiveconf");
    System.clearProperty("define");
    System.clearProperty("hivevar");
    assertNull(System.getProperty("_A"));
    String[] args = { "-hiveconf", "_A=B", "-define", "C=D", "-hivevar", "X=Y",
        "-S", "true", "-database", "testDb", "-e", "execString",  "-v", "true",
        "-h", "yahoo.host", "-p", "3000"};
    assertTrue(processor.process_stage1(args));
    assertEquals("B", System.getProperty("_A"));
    assertEquals("D", processor.getHiveVariables().get("C"));
    assertEquals("Y", processor.getHiveVariables().get("X"));

    CliSessionState sessionState = new CliSessionState(new HiveConf());
    processor.process_stage2(sessionState);
    assertEquals("testDb", sessionState.database);
    assertEquals("execString", sessionState.execString);
    assertEquals("yahoo.host", sessionState.host);
    assertEquals(3000, sessionState.port);
    assertEquals(0, sessionState.initFiles.size());
    assertTrue(sessionState.getIsVerbose());
    sessionState.setConf(null);
    assertTrue(sessionState.getIsSilent());

  }

  @Test
  public void testFiles() {
    OptionsProcessor processor = new OptionsProcessor();

    String[] args = {"-i", "f1", "-i", "f2","-f", "fileName",};
    assertTrue(processor.process_stage1(args));

    CliSessionState sessionState = new CliSessionState(new HiveConf());
    processor.process_stage2(sessionState);
    assertEquals("fileName", sessionState.fileName);
    assertEquals(2, sessionState.initFiles.size());
    assertEquals("f1", sessionState.initFiles.get(0));
    assertEquals("f2", sessionState.initFiles.get(1));

  }
}
