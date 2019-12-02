import java.util.*;
import java.io.*;

import com.jcraft.jsch.*;

public class Shell{

  public static void main(String[] arg){
    
    try {
      JSch jsch=new JSch();
      InputStreamReader s = new InputStreamReader(System.in);
      BufferedReader r = new BufferedReader(s);

      String host = "ric-edge-01.sci.pitt.edu";
      System.out.println("SSH into " + host + ": ");
      System.out.print("Enter Username: ");
      String user = r.readLine();
      System.out.print("Enter Password: ");
      //String pw=r.readLine();
      char[] pwa = System.console().readPassword();
      String pw = new String(pwa);

      Session session=jsch.getSession(user, host, 22);
      session.setPassword(pw);

      UserInfo ui = new MyUserInfo() {
        public void showMessage(String message) {
//          JOptionPane.showMessageDialog(null, message);
        }
        public boolean promptYesNo(String message) {
//          Object[] options={ "yes", "no" };
//          int foo=JOptionPane.showOptionDialog(null, 
//                                 l              message,
//                                               "Warning", 
//                                               JOptionPane.DEFAULT_OPTION, 
//                                               JOptionPane.WARNING_MESSAGE,
//                                               null, options, options[0]);
          //return foo==0;
          return true;
        }

        // If password is not given before the invocation of Session#connect(),
        // implement also following methods,
        //   * UserInfo#getPassword(),
        //   * UserInfo#promptPassword(String message) and
        //   * UIKeyboardInteractive#promptKeyboardInteractive()

      };

      session.setUserInfo(ui);
      session.setConfig("StrictHostKeyChecking", "no");
      session.connect(30000);   // making a connection with timeout.
      
      


      System.out.print("Enter output file: ");
      String output = r.readLine();

      List<String> cmdLines = new ArrayList<>();
      cmdLines.add("export JAVA_HOME=/usr/local/jdk1.8.0_101");
      cmdLines.add("export PATH=${JAVA_HOME}/bin:${PATH}");
      cmdLines.add("export HADOOP_CLASSPATH=/opt/cloudera/parcels/CDH/lib/hadoop/hadoop-common.jar:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-client-core.jar");
      cmdLines.add("hadoop fs -put Data/ .");
      cmdLines.add("rm proj.jar");
      cmdLines.add("hadoop fs -rm -r " + output);
      cmdLines.add("jar cvf projFinal.jar -C projFinal/ .");
      cmdLines.add("hadoop jar projFinal.jar Project Data " + output);
      cmdLines.add("hadoop fs -getmerge " + output + " collectedResults" + output);
      cmdLines.add("cat collectedResults" + output);
      cmdLines.add("echo Finished!");
      StringBuilder sb = new StringBuilder();
      for (String cmd : cmdLines) {
          if (sb.toString() != "") {
            sb.append("\n");
          }
          sb.append(cmd);
      }

      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setOutputStream(System.out, true);
      ((ChannelExec) channel).setCommand(sb.toString());
    //channel.setCommand("ls");
      channel.connect(3000);

      while (!channel.isClosed()) {
        System.out.print("*");
        Thread.currentThread().sleep(1000);
      }
      channel.disconnect();

      System.out.println("\n(Enter $ to exit)");
      while (true) {        
        System.out.print("Enter search term: ");
        String searchTerm = r.readLine();
        if ("$".equals(searchTerm)) {
            System.exit(0);
        }
        String searchCommand = "grep -E '^" + searchTerm + "\\s' collectedResults" + output;
        channel = (ChannelExec) session.openChannel("exec");
        channel.setOutputStream(System.out, true);
        channel.setCommand(searchCommand);
        channel.connect(3000);
        while (!channel.isClosed()) {
            Thread.currentThread().sleep(300);
        }
        channel.disconnect();
      }
      
    }
    catch (Exception e) {
      System.out.println(e);
    }
  }

  public static abstract class MyUserInfo
                          implements UserInfo, UIKeyboardInteractive{
    public String getPassword(){ return null; }
    public boolean promptYesNo(String str){ return false; }
    public String getPassphrase(){ return null; }
    public boolean promptPassphrase(String message){ return false; }
    public boolean promptPassword(String message){ return false; }
    public void showMessage(String message){ }
    public String[] promptKeyboardInteractive(String destination,
                                              String name,
                                              String instruction,
                                              String[] prompt,
                                              boolean[] echo){
      return null;
    }
  }
}