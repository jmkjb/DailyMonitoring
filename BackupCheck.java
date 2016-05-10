import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.codec.binary.Base64;

/*
 /ORACLE/JDK7/bin/javac -cp .:/ORACLE/BackupCheck/lib/commons-codec-1.10.jar:/ORACLE/BackupCheck/lib/ojdbc5.jar BackupCheck.java
 /ORACLE/JDK7/bin/java -cp .:/ORACLE/BackupCheck/lib/commons-codec-1.10.jar:/ORACLE/BackupCheck/lib/ojdbc5.jar BackupCheck /ORACLE/BackupCheck/srcdbinfo.properties /BACKUP /BACKUP/ORACLE/ARCH AWS_ICS_KR_DB jm.ha@isecommerce.co.kr jm.ha@isecommerce.co.kr,jm.ha@isecommerce.co.kr /ORACLE/BackupCheck/SendMessage.sh
  * 
  args[0] /ORACLE/BackupCheck/srcdbinfo.properties 
  args[1] /BACKUP 
  args[2] /BACKUP/ORACLE/ARCH 
  args[3] AWS_ICS_KR_DB 
  args[4] jm.ha@isecommerce.co.kr 
  args[5] jm.ha@isecommerce.co.kr,jm.ha@isecommerce.co.kr 
  args[6] /ORACLE/BackupCheck/SendMessage.sh
  args[7] GICS_TEAM 
 */

public class BackupCheck {

	static boolean chkctrlfile = false;
	static boolean chkarchfile = false;
	
	public static void main(String[] args) {
		
		//System.out.println(args[0]);
		GetDBSrcInfo(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
	}
	
	public static void GetDBSrcInfo(String conninfo, String dir, String archdir, String dbname, String frommail, String tomail, String shellfile, String groupname) {
		
		FileInputStream fis = null; 
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rssrcfile = null;
		
		String datafilelist = "SELECT FILE_NAME FROM DBA_DATA_FILES";
		String datafilecount = "SELECT COUNT(*) AS FILE_COUNT FROM DBA_DATA_FILES";
		
		try {
			//Get Connection Info
			Properties props = new Properties();
			fis = new FileInputStream(conninfo);
			props.load(new java.io.BufferedInputStream(fis));
			
			String dbclass = props.getProperty("SrcDBClass");
			String dburl = props.getProperty("SrcDBConnectUrl");
			String user = props.getProperty("SrcDBUserId");
			String passwd = props.getProperty("SrcDBUserPasswd");
			
			//System.out.println("dbclass : " + dbclass + ", dburl : " + dburl + ", user : " + user + ",  passwd : " + DecodingChar(passwd));
			
			//Get File Lists
			conn = DriverManager.getConnection(dburl, user, DecodingChar(passwd));
			pstmt = conn.prepareStatement(datafilecount);
			rssrcfile = pstmt.executeQuery();
			int filecount = 0;
			
			while (rssrcfile.next()) {
				filecount = rssrcfile.getInt("FILE_COUNT");
			}
			
			//System.out.println("Data File Count : " + filecount);
			
			pstmt = conn.prepareStatement(datafilelist);
			rssrcfile = pstmt.executeQuery();
			int errorcnt = 0;
			
			while (rssrcfile.next()) {
				File chkfile = new File(rssrcfile.getString("FILE_NAME"));
				//System.out.println("Data File : " + chkfile.getAbsoluteFile() + ", Size : " + chkfile.length());
				
				File bkpdatafile = new File(dir+chkfile);
				//System.out.println("Backup Data File : " + bkpdatafile.getAbsoluteFile() + ", Backup Data Size : " + bkpdatafile.length());
				
				if ( bkpdatafile.length() >= chkfile.length() && bkpdatafile.length() > 0 ) {
					System.out.println("Backup is Completed!!!");
					filecount--;
				} else {
					System.out.println("Backup is Incompleted!!!");
					errorcnt++;
				}
			}
			
			//System.out.println("Error Count : " + errorcnt + ", Execute File Count : " + filecount);
			
			ChkControl(dir);
			ChkArchive(archdir);
			
			if (chkctrlfile) {
				System.out.println("Pass Control File !!!");
			} else {
				System.out.println("Error Control File !!!");
			}
			
			if (chkarchfile) {
				System.out.println("Pass Archive File !!!");
			} else {
				System.out.println("Error Archive File !!!");
			}
			
			String[] tomaillist = tomail.split(",");
			
			if (errorcnt != 0 || filecount != 0 || !chkctrlfile || !chkarchfile) {
				System.out.println("Backup is Incompleted!!!");
				
				for (int i = 0; i < tomaillist.length; i++) {
					sendCheckDBFile("mail.isecommerce.co.kr", frommail, tomaillist[i], dbname, "Fail");
				}
				
				ShellCmdExec(shellfile, groupname, dbname, "BackupCompleted!!!");
			} else if (errorcnt == 0 && filecount == 0 && chkctrlfile && chkarchfile) {
				System.out.println("Backup is Completed!!!");
				
				for (int i = 0; i < tomaillist.length; i++) {
					sendCheckDBFile("mail.isecommerce.co.kr", frommail, tomaillist[i], dbname, "Success");
				}
				
				ShellCmdExec(shellfile, groupname, dbname, "BackupCompleted!!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try{
				if(conn != null) { conn.close(); }
				if(fis != null) { fis.close(); }
			} catch (Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public static final String EncodingChar(String str) {
		byte[] encoded = Base64.encodeBase64(str.getBytes());
		String reStr = new String(encoded);
		return reStr;
	}

	public static final String DecodingChar(String str) {
		byte[] decoded = Base64.decodeBase64(str);
		String reStr = new String(decoded);
		return reStr;
	}
	
	public static void ChkControl(String dir) {
		
		File bkpdir = new File(dir); 
		File[] fileList = bkpdir.listFiles(); 
		
		try {
			for (int i = 0 ; i < fileList.length ; i++) {
				File file = fileList[i]; 
				
				if (file.isFile()) {
					if ((file.getName().toLowerCase()).contains("control")) {
						chkctrlfile = true;
						//System.out.println("Control File : " + file.getName());
						break;
					}
				}else if (file.isDirectory()) {
					ChkControl(file.getCanonicalPath().toString());
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void ChkArchive(String dir) {
		
		try{
			Calendar cal = new GregorianCalendar(Locale.getDefault());
		    cal.setTime(new Date());
		    cal.add(Calendar.DAY_OF_YEAR, 0);
		    SimpleDateFormat fm = new SimpleDateFormat("yyyy-MM-dd");
		    String strdate = fm.format(cal.getTime());
			
			File file = new File(dir);

			if (!file.isDirectory()) {
				System.out.println("Directory is not Exists");
				System.exit(1);
			}

			File[] list = file.listFiles();
			Arrays.sort(list, new ModifiedDate());
			
			SimpleDateFormat transFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date chkdate = transFormat.parse(strdate);
			
			for (File fileentry : list) {
				if (fileentry.isFile()) {
					Date filedate = new Date(fileentry.lastModified());
					String strfiledate = fm.format(filedate);
					
					Date filedate2 = transFormat.parse(strfiledate);
					
					int compare = chkdate.compareTo( filedate2 );
					
					if (compare >= 0) {
						//System.out.println("Check Date : " + strdate + ", File Date : " + strfiledate + ", Compare Count : " + compare);
						chkarchfile = true;
					} else {
						//System.out.println("Check Date : " + strdate + ", File Date : " + strfiledate + ", Compare Count : " + compare);
						chkarchfile = false;
					}
					
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void sendCheckDBFile (String sHost, String sFromMail, String sToMail, String sDBName, String sStatus) {

		String host = sHost;
		String from = sFromMail;
		String to = sToMail;
		String dbname = sDBName;
		String status = sStatus;
		boolean debug = false;
		
		String msgText1 = "Sending a file Contents.\n"
						 + dbname + " " + " DB Backup Status Check Mail.." + status;
		
		String subject = getDate() + " " + dbname + " " + " Backup Check File";
		
		Properties props = System.getProperties();
		props.put("mail.smtp.host", host);
		
		Session session = Session.getInstance(props, null);
		session.setDebug(debug);
		
		try {
		    MimeMessage msg = new MimeMessage(session);
		    msg.setFrom(new InternetAddress(from));
		    InternetAddress[] address = {new InternetAddress(to)};
		    msg.setRecipients(Message.RecipientType.TO, address);
		    msg.setSubject(subject);
	
		    MimeBodyPart mbp1 = new MimeBodyPart();
		    mbp1.setText(msgText1);
	
		    //MimeBodyPart mbp2 = new MimeBodyPart();
	
		    Multipart mp = new MimeMultipart();
		    mp.addBodyPart(mbp1);
		    //mp.addBodyPart(mbp2);
	
		    msg.setContent(mp);

		    msg.setSentDate(new Date());
		    
		    Transport.send(msg);
		}catch(MessagingException mex){
		    mex.printStackTrace();
		    
		    Exception ex = null;
		    
		    if((ex = mex.getNextException()) != null) {
		    	ex.printStackTrace();
		    }
		}catch(Exception e){
		    e.printStackTrace();
		}
    }
	
	public static final String getDate() {

		String currTime = null;
		Date myDate = new Date();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMddHHmm");
		myDate.setTime(System.currentTimeMillis());
		currTime = dayTime.format(myDate);

		return currTime;
	}

	/*
		/ORACLE/BackupCheck/SendMessage.sh --execute shell file
		"GICS_TEAM" --telegram group name
		"ICS_KRDB" --dbname
		"BACKUP COMPLETED" --text
    */
	
	public static void ShellCmdExec(String shfile, String groupname, String dbname, String sendtext) {

		try {
			String sCmd = shfile + " " + groupname + " " + dbname + " " + sendtext;
			System.out.println("Telegram Send Message : " + sCmd);
			
			Runtime runtime = Runtime.getRuntime();
			Process process = runtime.exec(sCmd);
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			
			String line = "";
			
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}		
	}
}

class ModifiedDate implements Comparator<File> {

	public int compare(File f1, File f2) {
		if (f1.lastModified() < f2.lastModified())
			return 1;

		if (f1.lastModified() == f2.lastModified())
			return 0;

		return -1;
	}
}
