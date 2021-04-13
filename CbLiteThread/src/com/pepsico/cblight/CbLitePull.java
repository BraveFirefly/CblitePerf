package com.pepsico.cblight;

import com.couchbase.lite.Authenticator;
import com.couchbase.lite.BasicAuthenticator;
import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DataSource;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseConfiguration;
import com.couchbase.lite.DocumentFlag;
import com.couchbase.lite.Endpoint;
import com.couchbase.lite.Expression;
import com.couchbase.lite.ListenerToken;
import com.couchbase.lite.Meta;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryBuilder;
import com.couchbase.lite.Replicator;
import com.couchbase.lite.ReplicatorConfiguration;
import com.couchbase.lite.ResultSet;
import com.couchbase.lite.SelectResult;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CbLitePull extends Thread {	

	private int steadyState, iterations, lastVal;
	private String sgUrl, cerf, dbName, userName, userPass, userChannel, outputFilePath, errorFilePath;

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
	
	public CbLitePull() {
	}
	
	public CbLitePull(ThreadGroup argThreadGroup, String argThreadName, CbLiteGUI argCblGui, int argSteadyState, int argIterations, String argSgUrl, String argCerf, String argUserName, String argUserPass, String argUserChannel, String argDbName) {
		super(argThreadGroup, argThreadName);
		threadGroup = argThreadGroup;
		cblgui = argCblGui;
		iterations = argIterations;
		sgUrl = argSgUrl;
		cerf = argCerf;
		userName = argUserName;
		userPass = argUserPass;
		userChannel = argUserChannel;
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
				URLEndpoint uRLEndpoint = new URLEndpoint(new URI(sgUrl));
				ReplicatorConfiguration replConfig = new ReplicatorConfiguration(database, (Endpoint)uRLEndpoint);
				replConfig.setReplicatorType(ReplicatorConfiguration.ReplicatorType.PULL);
				replConfig.setPullFilter((document, flags) -> !flags.contains(DocumentFlag.DocumentFlagsDeleted));
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
				long firstDbSize = FileUtils.sizeOfDirectory(new File(DB_PATH + "\\" + dbName + ".cblite2"));
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
				String out = printCBLiteRecords(database, userChannel);
				String[] records = out.split(";");
				double rowCount = cblgui.getRowCount();
				if(!this.isInterrupted()) {
					double up = rowCount/lastVal;
					int progVal = (int)(up*100);
					cblgui.setBar(progVal);
					cblgui.incAndSetSuccess();
					FileWriter fileWriter = new FileWriter(outputFilePath, true);
					fileWriter.write(logTime(start) + ";" + threadName + ";" + (int)rowCount + ";" + userName + ";" + dbName + ";" + records[0] + ";" + records[1] + ";" + records[2] + ";"+  String.format("%.3f", sec) +";"+ String.format("%.2f", after) + "\n");
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
				if(anyException&&r!=0) {
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
	
	private String printCBLiteRecords(Database database, String channel) {
		int num1 = 0;
		Query query = QueryBuilder.select(new SelectResult[] { (SelectResult)SelectResult.expression((Expression)Meta.id) }).from((DataSource)DataSource.database(database)).where(Expression.property("RteId").equalTo(Expression.string(channel)));
		try {
			ResultSet result2 = query.execute();
			num1 = result2.allResults().size();
		} catch (CouchbaseLiteException exception) {
			exception.printStackTrace();
		}
		return Long.toString(database.getCount()) + ";" + channel + ";" + Integer.toString(num1);
	}
	
	private static String logTime(Instant ins) {
		LocalDateTime ldt = LocalDateTime.ofInstant(ins, ZoneId.systemDefault());
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
		return ldt.format(formatter);
	}
	
}