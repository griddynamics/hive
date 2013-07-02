/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.errors;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaskLogProcessor {
  
  private File writeTestLog(String id, String content) throws IOException {
    // Put the script content in a temp file
    File scriptFile = File.createTempFile(getClass().getName() + "-" + id + "-", ".log");
    scriptFile.deleteOnExit();
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    os.print(content);
    os.close();
    return scriptFile;
  }
  
  private String toString(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw, false); 
    t.printStackTrace(pw);
    pw.close();
    return sw.toString();
  }
  
  @Test
  public void testGetStackTraces() throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.set(HiveConf.ConfVars.HIVEQUERYSTRING.varname, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);

    Throwable oome = new OutOfMemoryError("java heap space");
    File log1File = writeTestLog("1", toString(oome));
    taskLogProcessor.addTaskAttemptLogUrl(log1File.toURI().toURL().toString());

    Throwable t2 = new InvocationTargetException(new IOException(new NullPointerException()));
    File log2File = writeTestLog("2", toString(t2));
    taskLogProcessor.addTaskAttemptLogUrl(log2File.toURI().toURL().toString());

    Throwable t3 = new EOFException();
    String content = "line a\nlineb\n" + toString(t3) + " line c\nlineD\n";
    File log3File = writeTestLog("3", content);
    taskLogProcessor.addTaskAttemptLogUrl(log3File.toURI().toURL().toString());
    
    List<List<String>> stackTraces = taskLogProcessor.getStackTraces();
    assertEquals(3, stackTraces.size());
    
    assertEquals(26, stackTraces.get(0).size());
    assertEquals(30, stackTraces.get(1).size());
    assertEquals(26, stackTraces.get(2).size());
  }
  
  @Test
  public void testScriptErrorHeuristic() throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.set(HiveConf.ConfVars.HIVEQUERYSTRING.varname, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);
    
    String content = "line a\nlineb\n" + "Script failed with code 74" + " line c\nlineD\n";
    File log3File = writeTestLog("1", content);
    taskLogProcessor.addTaskAttemptLogUrl(log3File.toURI().toURL().toString());
    
    List<ErrorAndSolution> errList = taskLogProcessor.getErrors();
    assertEquals(1, errList.size());
    
    final ErrorAndSolution eas = errList.get(0);
    
    String error = eas.getError();
    assertNotNull(error);
    // check that the error code is present in the error description: 
    assertTrue(error.indexOf("74") >= 0);
    
    String solution = eas.getSolution();
    assertNotNull(solution);
    assertTrue(solution.length() > 0);
  }

  @Test
  public void testDataCorruptErrorHeuristic() throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.set(HiveConf.ConfVars.HIVEQUERYSTRING.varname, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);
    
    String badFile1 = "hdfs://localhost/foo1/moo1/zoo1"; 
    String badFile2 = "hdfs://localhost/foo2/moo2/zoo2"; 
    String content = "line a\nlineb\n" 
       + "split: " + badFile1 + " is very bad.\n" 
       + " line c\nlineD\n" 
       + "split: " + badFile2 + " is also very bad.\n" 
       + " java.io.EOFException: null \n" 
       + "line E\n";
    File log3File = writeTestLog("1", content);
    taskLogProcessor.addTaskAttemptLogUrl(log3File.toURI().toURL().toString());
    
    List<ErrorAndSolution> errList = taskLogProcessor.getErrors();
    assertEquals(1, errList.size());
    
    final ErrorAndSolution eas = errList.get(0);
    
    String error = eas.getError();
    assertNotNull(error);
    // check that the error code is present in the error description: 
    assertTrue(error.contains(badFile1) || error.contains(badFile2));
    
    String solution = eas.getSolution();
    assertNotNull(solution);
    assertTrue(solution.length() > 0);
  }
  
  @Test
  public void testMapAggrMemErrorHeuristic() throws Exception {
    JobConf jobConf = new JobConf();
    jobConf.set(HiveConf.ConfVars.HIVEQUERYSTRING.varname, "select * from foo group by moo;");

    final TaskLogProcessor taskLogProcessor = new TaskLogProcessor(jobConf);

    Throwable oome = new OutOfMemoryError("java heap space");
    File log1File = writeTestLog("1", toString(oome));
    taskLogProcessor.addTaskAttemptLogUrl(log1File.toURI().toURL().toString());
    
    List<ErrorAndSolution> errList = taskLogProcessor.getErrors();
    assertEquals(1, errList.size());
    
    final ErrorAndSolution eas = errList.get(0);
    
    String error = eas.getError();
    assertNotNull(error);
    // check that the error code is present in the error description: 
    assertTrue(error.contains("memory"));
    
    String solution = eas.getSolution();
    assertNotNull(solution);
    assertTrue(solution.length() > 0);
    String confName = HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY.toString();
    assertTrue(solution.contains(confName));
  }
  
}
