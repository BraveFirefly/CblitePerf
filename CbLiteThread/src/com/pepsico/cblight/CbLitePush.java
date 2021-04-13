package com.pepsico.cblight;

import com.couchbase.lite.Authenticator;
import com.couchbase.lite.BasicAuthenticator;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.Endpoint;
import com.couchbase.lite.ListenerToken;
import com.couchbase.lite.MutableArray;
import com.couchbase.lite.MutableDictionary;
import com.couchbase.lite.MutableDocument;
import com.couchbase.lite.Replicator;
import com.couchbase.lite.ReplicatorConfiguration;
import com.couchbase.lite.URLEndpoint;
import java.io.FileWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class CbLitePush extends Thread {	

	private int steadyState, iterations, lastVal;
	private final int pushDocs = 3;
	private String sgUrl, cerf, dbName, userName, userPass, outputFilePath, errorFilePath;

	void setValues(String outputFilePath, String errorFilePath, int lastVal) {
		this.outputFilePath = outputFilePath;
		this.errorFilePath = errorFilePath;
		this.lastVal = lastVal;
	}
	
	private Database database = null;
	private Replicator replicator = null;
	private ListenerToken token = null;
	CbLiteGUI cblgui;
	ThreadGroup threadGroup;
	
	public CbLitePush() {
	}
	
	public CbLitePush(ThreadGroup argThreadGroup, String argThreadName, CbLiteGUI argCblGui, int argSteadyState, int argIterations, String argSgUrl, String argCerf, String argUserName, String argUserPass, String argUserChannel, String argDbName) {
		super(argThreadGroup, argThreadName);
		threadGroup = argThreadGroup;
		cblgui = argCblGui;
		iterations = argIterations;
		sgUrl = argSgUrl;
		cerf = argCerf;
		userName = argUserName;
		userPass = argUserPass;
		dbName = argDbName;
		steadyState = (argSteadyState*1000);
		try {
			CouchbaseLite.init();
		}catch(Exception e) {
			System.out.println(e);
		}
	}

	public void run() {
		for(int r=1; r<=iterations; r++) {
			String threadName = Thread.currentThread().getName() + "-" + Integer.toString(r);
			String DB_PATH = cblgui.getMainPath() + "\\Resources\\" + threadName;
			boolean anyException = false;
			try {
				DatabaseConfiguration config = new DatabaseConfiguration();
				config.setDirectory(DB_PATH);
				database = new Database(dbName, config);
				long firstDbSize = FileUtils.sizeOfDirectory(new File(DB_PATH + "\\" + dbName + ".cblite2"));
				for(int docCount=1;docCount<=pushDocs;docCount++) {
					String docID = getDocumentID(r, docCount);
					System.out.println(docID);
				    MutableDocument md = new MutableDocument(docID);
				    String id = md.getId();
			        md.setString("Id", id);
			        createDocument(md);
				    database.save(md);
				}
				URLEndpoint uRLEndpoint = new URLEndpoint(new URI(sgUrl));
				ReplicatorConfiguration replConfig = new ReplicatorConfiguration(database, (Endpoint)uRLEndpoint);
				replConfig.setReplicatorType(ReplicatorConfiguration.ReplicatorType.PUSH);
				if (System.getProperty(cerf) != null) {
					  InputStream is = null;
					  byte[] cert = null;
					  try {
						  is = new FileInputStream(System.getProperty(cerf));
						  cert = IOUtils.toByteArray(is);
						  if(is!=null) {
							  is.close();
						  }
						  replConfig.setPinnedServerCertificate(cert);						  
					  }catch(FileNotFoundException exception) {
						  exception.printStackTrace();
						  anyException = true;
					  }catch(IOException exception) {
						  exception.printStackTrace();
						  anyException = true;
					  }finally {
						  if(anyException) {
							  cblgui.worker.cancel(true);
							  try {
				                  if (this.database != null)
				                	  this.database.close(); 
				              } catch (CouchbaseLiteException exception) {
				                  exception.printStackTrace();
				              } catch (Exception exception) {
				                  exception.printStackTrace();
				              }
							  break;
						  }
					  }
				}
				replConfig.setAuthenticator((Authenticator)new BasicAuthenticator(userName, userPass));
				replConfig.setContinuous(false);
				replicator = new Replicator(replConfig);
				token = replicator.addChangeListener(change -> {
					try {
						Instant now = Instant.now();
						CouchbaseLiteException error = change.getStatus().getError();
						if(error!=null) {
							int errorCode = error.getCode();
							System.err.println("Error code ::  " + errorCode);
							cblgui.incAndSetError();
							if(!cblgui.worker.isCancelled()) {
//								cblgui.setErrorLogs(logTime(now) + ";" + threadName + ";" + userName + ";" + dbName + ";" + String.valueOf(errorCode) + "\n");
								FileWriter fw1 = new FileWriter(errorFilePath, true);
								fw1.write(logTime(now) + ";" + threadName + ";" + userName + ";" + dbName + ";" + String.valueOf(errorCode) + "\n");
								fw1.close();
							}
						}
						if(cblgui.worker.isCancelled()) {
							Thread.currentThread().interrupt();
						}
					}catch(Exception e) {
						e.printStackTrace();
					}
				});
				Instant start= Instant.now();
				replicator.start();
				while (replicator.getStatus().getActivityLevel() != Replicator.ActivityLevel.STOPPED) {
					Thread.sleep(0);
				}
				Instant end= Instant.now();
				replicator.stop();
				long lastDbSize = FileUtils.sizeOfDirectory(new File(DB_PATH + "\\" + dbName + ".cblite2"));
				double after = ((double)(lastDbSize-firstDbSize)/(1024*1024));
				Duration timeElapsed= Duration.between(start,end);
				final float sec = timeElapsed.toMillis() / 1000.0f;
				double rowCount = cblgui.getRowCount();
				if(!this.isInterrupted()) {
					double up = rowCount/lastVal;
					int progVal = (int)(up*100);
					cblgui.setBar(progVal);
					cblgui.incAndSetSuccess();
					FileWriter fileWriter = new FileWriter(outputFilePath, true);
					fileWriter.write(logTime(start) + ";" + threadName + ";" + (int)rowCount + ";" + userName + ";" + dbName + ";" + pushDocs + ";"+  String.format("%.3f", sec) +";"+ String.format("%.2f", after) + "\n");
					fileWriter.close();
				}
				cblgui.incRowCount();
				replicator.removeChangeListener(token);
				replicator.stop();
				database.delete();
				if(r<iterations) {
					Thread.sleep(steadyState);
				}
			}catch(CouchbaseLiteException exception) {
				exception.printStackTrace();
				anyException = true;
			} catch (FileNotFoundException exception) {
				exception.printStackTrace();
				anyException = true;
			}catch(InterruptedException exception) {
				exception.printStackTrace();
				anyException = true;
			}catch(URISyntaxException exception) {
				exception.printStackTrace();
				anyException = true;
			}catch(IOException exception) {
				exception.printStackTrace();
				anyException = true;
			}catch(Exception exception) {
				exception.printStackTrace();
				anyException = true;
			}finally {
				if(anyException) {
					try {
						if(replicator!=null) {
							replicator.removeChangeListener(token);
							replicator.stop();
							while (replicator.getStatus().getActivityLevel() != Replicator.ActivityLevel.STOPPED) {
								Thread.sleep(1000);
							}
						}
						if(database!=null) {
							database.close();
						}
					} catch (InterruptedException exception) {
						exception.printStackTrace();
					} catch (CouchbaseLiteException exception) {
						exception.printStackTrace();
					} catch(Exception exception) {
						exception.printStackTrace();
					}
					cblgui.worker.cancel(true);
					break;
				}
			}
		}
	}
	
	private static String logTime(Instant ins) {
		LocalDateTime ldt = LocalDateTime.ofInstant(ins, ZoneId.systemDefault());
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
		return ldt.format(formatter);
	}
	
	private String getDocumentID(int iteration, int docCount) {
		LocalDateTime ldt = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmmss");
		String[] threads = Thread.currentThread().getName().split("-");
		return "CustOrder" + threads[1] + String.valueOf(iteration) + String.valueOf(docCount) + userName + "::" + ldt.format(formatter);
	}
	
	private MutableDocument createDocument(MutableDocument md) {
		Date date = new Date(); 
		date.from(Instant.now());
		
		for(int count=1;count<=50;count++) {
			MutableDictionary dic1 = new MutableDictionary()
					.setString("LocTypCdv"+String.valueOf(count), "03")
					.setInt("LocId"+String.valueOf(count), 2421)
					.setString("CtryCd"+String.valueOf(count), "157")
					.setString("LocNm"+String.valueOf(count), "OK MEGA DC");
			
			MutableDictionary dic2 = new MutableDictionary()
					.setDouble("GrssSoldAmt"+String.valueOf(count), 547.32)
					.setDouble("WhseQty"+String.valueOf(count), 386)
					.setDouble("SoldExtnddEaQty"+String.valueOf(count), 407)
					.setDouble("TaxblAmt"+String.valueOf(count), 547.32)
					.setDouble("NetTaxAmt"+String.valueOf(count), 0)
					.setDouble("NetPrmtnAmt"+String.valueOf(count), 0)
					.setDouble("NetDueAmt"+String.valueOf(count), 547.32);
			
			int[] evntNum = {321234, 319438};
			
			MutableArray arr = new MutableArray();
			for(int num=0;num<2;num++) {
				MutableDictionary dicArr = new MutableDictionary()
						.setInt("CaldrNum"+String.valueOf(count), 506221)
						.setInt("GrpNum"+String.valueOf(count), num+1)
						.setInt("EvntNum"+String.valueOf(count), evntNum[num])
						.setInt("PrmtnTyp"+String.valueOf(count), 2);
				arr.addDictionary(dicArr);
			}
			
			int[] MfrrPrfxCdv = {28400, 17802};
			
			String[] MtrlUomId1 = {"06979401", "06979801", "05369200", "10124300", "10062800", "10063500", "10063200"};
			int[] OrdrQty1 = {1, 2, 3, 4, 8, 8, 8};
			double[] SgstdRtlPrcAmt1 = {0.99, 0.99, 3.99, 1.89, 1.89, 1.89, 1.89};
			int[] PlanSaleQty1 = {8, 16, 3, 4, 8, 8, 8};
			
			String[] MtrlUomId2 = {"08373500", "08103300"};
			int[] OrdrQty2 = {2, 2};
			double[] SgstdRtlPrcAmt2 = {3.99, 3.79};
			int[] PlanSaleQty2 = {2, 2};
			
			MutableArray SaleLnItmDtl1 = new MutableArray();
			for(int m1=0;m1<7;m1++) {
				MutableDictionary md1 = new MutableDictionary();
				md1.setString("MtrlUomId"+String.valueOf(count), MtrlUomId1[m1]);
				md1.setInt("OrdrQty"+String.valueOf(count), OrdrQty1[m1]);
				md1.setDouble("SgstdRtlPrcAmt"+String.valueOf(count), SgstdRtlPrcAmt1[m1]);
				md1.setInt("PlanSaleQty"+String.valueOf(count), PlanSaleQty1[m1]);
				SaleLnItmDtl1.addDictionary(md1);
			}
			
			MutableArray SaleLnItmDtl2 = new MutableArray();
			for(int m2=0;m2<2;m2++) {
				MutableDictionary md1 = new MutableDictionary();
				md1.setString("MtrlUomId"+String.valueOf(count), MtrlUomId2[m2]);
				md1.setInt("OrdrQty"+String.valueOf(count), OrdrQty2[m2]);
				md1.setDouble("SgstdRtlPrcAmt"+String.valueOf(count), SgstdRtlPrcAmt2[m2]);
				md1.setInt("PlanSaleQty"+String.valueOf(count), PlanSaleQty2[m2]);
				SaleLnItmDtl2.addDictionary(md1);
			}
			
			MutableDictionary muteDic1 = new MutableDictionary();
			muteDic1.setInt("MfrrPrfxCdv"+String.valueOf(count), MfrrPrfxCdv[0]);
			muteDic1.setArray("SaleLnItmDtl"+String.valueOf(count), SaleLnItmDtl1);
			
			MutableDictionary muteDic2 = new MutableDictionary();
			muteDic2.setInt("MfrrPrfxCdv"+String.valueOf(count), MfrrPrfxCdv[1]);
			muteDic2.setArray("SaleLnItmDtl"+String.valueOf(count), SaleLnItmDtl2);
			
			MutableArray MtrlUomList = new MutableArray();
			MtrlUomList.addDictionary(muteDic1);
			MtrlUomList.addDictionary(muteDic2);
			
			md.setDictionary("SrcLoc"+String.valueOf(count), dic1)
				.setDictionary("DocTot"+String.valueOf(count), dic2)
				.setArray("PrmtnDtl"+String.valueOf(count), arr)
				.setArray("MtrlUomList"+String.valueOf(count), MtrlUomList);
		}

		md.setString("$Type", "CustOrder")
				.setString("$DocVrsn", "1.0")
				.setString("$MdfdById", "infgmw")
				.setDate("$MdfdTmstmp", date)
				.setString("$Channels", "")
				.setDate("DocStrtDtm", date)
				.setDate("DocEndDtm", date)
				.setString("DocNum", "17063465")
				.setString("RteId", "47400")
				.setString("RteCtryCd", "157")
				.setString("CoilId", "123456")
				.setString("WrkfrcId", "03295240")
				.setString("GtmuCustId", "42182983")
				.setString("CustPaymtMthdCdv", "0")
				.setString("DocPaymtMthdCdv", "0")
				.setInt("TaxPct", 0)
				.setString("PoNum", "12345678")
				.setString("CntrcId", "1234")
				.setString("ScheddDlvryDt", "2020-06-16");

		return md;
	}
	
}