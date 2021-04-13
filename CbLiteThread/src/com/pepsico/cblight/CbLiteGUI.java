package com.pepsico.cblight;

import com.couchbase.lite.URLEndpoint;
import com.couchbase.lite.utils.FileUtils;
import java.awt.EventQueue;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.SwingWorker;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.border.LineBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.Color;
import javax.swing.JProgressBar;

public class CbLiteGUI implements ActionListener {

	private JFrame frmCouchbaseLitePerf;
	private JLabel totalUsersLabel;
	private JTextField totalUsersText;
	private JLabel inEveryLabel;
	private JTextField rampTimeText;
	private JLabel iterationsLabel;
	private JTextField iterationsText;
	private JLabel steadyStateLabel;
	private JTextField steadyStateText;
	private JLabel pacingLabel;
	private JTextField pacingText;
	private JLabel sgUrlLabel;
	private JTextField sgUrlText;
	private JLabel cerfPathLabel;
	private JLabel cerfPath;
	private JButton cerfPathButton;
	private JLabel userCredLabel;
	private JLabel userCredPath;
	private JButton userCredButton;
	private JLabel dbNameLabel;
	private JLabel dbNamePath;
	private JButton dbNameButton;
	private JPanel statusPanel;
	private JButton pullButton;
	private JButton pushButton;
	private JButton pushAndPullButton;
	private JButton stopButton;
	private JButton clearButton;
	private JLabel statusLabel;
	private JProgressBar progressBar;
	private JLabel errorLabel;
	private JLabel errorText;
	private JLabel passedLabel;
	private JLabel passedText;
	private JTextField usersText;
	private JLabel inSecondsLabel;
	private JLabel noOfUsersLabel;

	private final ThreadGroup threadGroup = new ThreadGroup("UserThreads");
	private int rowCount = 1, errorCount = 0, passedCount = 0;
	private final String mainPath = "D:\\CbLite";
	private final String outputPath = getMainPath() + "\\Output\\" + currentDateAndTime("date");
	
	public String getMainPath() {
		return mainPath;
	}
	
	private String sgUrl = "", cerf = "", userCred = "", dbName = "", inputError = "Incorrect Input - ", blankError = "Field empty - ";
	private int totalUsers = 0, users = 0, iterations = 0, steadyState = 0, user_count = 0, db_count = 0, total_users = 0, total_db = 0;
	private double rampTime = 0; 
	
	private List<String> DB_NAME_LIST = new ArrayList<String>();
	private List<String> DB_USER_LIST = new ArrayList<String>();
	private List<String> DB_PASS_LIST = new ArrayList<String>();
	private List<String> DB_CHANNEL_LIST = new ArrayList<String>();
	
	SwingWorker<Void, Void> worker;

	synchronized int getRowCount() {
		return rowCount;
	}

	synchronized void incRowCount() {
		this.rowCount += 1;
	}
	
	synchronized int getErrorCount() {
		return errorCount;
	}
	
	synchronized void refreshErrorText() {
		this.errorText.setText(String.valueOf(this.errorCount));
	}

	synchronized void incAndSetError() {
		this.errorCount += 1;
		refreshErrorText();
	}
	
	synchronized int getSuccessCount() {
		return this.passedCount;
	}
	
	synchronized void refreshPassedText() {
		this.passedText.setText(String.valueOf(this.passedCount));
	}

	synchronized void incAndSetSuccess() {
		this.passedCount += 1;
		refreshPassedText();
	}

	/**
	 * Launch the application.
	 * @throws InterruptedException 
	 * @throws InvocationTargetException 
	 */
	public static void main(String[] args){
		
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					CbLiteGUI window = new CbLiteGUI();
					window.frmCouchbaseLitePerf.setVisible(true);
					File newFile = new File(window.getMainPath());
					if(!newFile.exists()) {
						newFile.mkdir();
					}
					File newOutputFile = new File(window.getMainPath() + "\\Output");
					if(!newOutputFile.exists()) {
						newOutputFile.mkdir();
					}
					File newDateFile = new File(window.outputPath);
					if(!newDateFile.exists()) {
						newDateFile.mkdir();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
	}

	/**
	 * Create the application.
	 */
	public CbLiteGUI() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmCouchbaseLitePerf = new JFrame();
		frmCouchbaseLitePerf.setResizable(false);
		frmCouchbaseLitePerf.setTitle("Couchbase Lite Perf Utility");
		frmCouchbaseLitePerf.setBounds(100, 100, 577, 493);
		frmCouchbaseLitePerf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frmCouchbaseLitePerf.getContentPane().setLayout(null);
		
		totalUsersLabel = new JLabel("Total no of users");
		totalUsersLabel.setHorizontalAlignment(SwingConstants.TRAILING);
		totalUsersLabel.setBounds(33, 24, 118, 14);
		frmCouchbaseLitePerf.getContentPane().add(totalUsersLabel);
		
		totalUsersText = new JTextField();
		totalUsersLabel.setLabelFor(totalUsersText);
		totalUsersText.setBounds(161, 21, 96, 20);
		frmCouchbaseLitePerf.getContentPane().add(totalUsersText);
		totalUsersText.setColumns(10);
		
		iterationsLabel = new JLabel("No of iterations");
		iterationsLabel.setLabelFor(iterationsText);
		iterationsLabel.setHorizontalAlignment(SwingConstants.TRAILING);
		iterationsLabel.setBounds(302, 24, 127, 14);
		frmCouchbaseLitePerf.getContentPane().add(iterationsLabel);
		
		iterationsText = new JTextField();
		iterationsText.setBounds(439, 21, 96, 20);
		frmCouchbaseLitePerf.getContentPane().add(iterationsText);
		iterationsText.setColumns(10);
		iterationsText.getDocument().addDocumentListener(new DocumentListener() {

			@Override
			public void insertUpdate(DocumentEvent e) {
				setPacing();
			}

			@Override
			public void removeUpdate(DocumentEvent e) {
				setPacing();				
			}

			@Override
			public void changedUpdate(DocumentEvent e) {
				setPacing();				
			}
			
		});
		
		steadyStateLabel = new JLabel("Steady State (in mins)");
		steadyStateLabel.setHorizontalAlignment(SwingConstants.TRAILING);
		steadyStateLabel.setBounds(10, 55, 141, 14);
		frmCouchbaseLitePerf.getContentPane().add(steadyStateLabel);
		
		steadyStateText = new JTextField();
		steadyStateLabel.setLabelFor(steadyStateText);
		steadyStateText.setBounds(161, 52, 96, 20);
		frmCouchbaseLitePerf.getContentPane().add(steadyStateText);
		steadyStateText.setColumns(10);
		steadyStateText.getDocument().addDocumentListener(new DocumentListener() {

			@Override
			public void insertUpdate(DocumentEvent e) {
				setPacing();
			}

			@Override
			public void removeUpdate(DocumentEvent e) {
				setPacing();				
			}

			@Override
			public void changedUpdate(DocumentEvent e) {
				setPacing();				
			}
			
		});
		
		pacingLabel = new JLabel("Pacing Time (in sec)");
		pacingLabel.setHorizontalAlignment(SwingConstants.TRAILING);
		pacingLabel.setBounds(302, 55, 127, 14);
		frmCouchbaseLitePerf.getContentPane().add(pacingLabel);
		
		pacingText = new JTextField();
		pacingText.setText("0");
		pacingLabel.setLabelFor(pacingText);
		pacingText.setEditable(false);
		pacingText.setBounds(439, 52, 96, 20);
		frmCouchbaseLitePerf.getContentPane().add(pacingText);
		pacingText.setColumns(10);
		
		noOfUsersLabel = new JLabel("Number of users");
		noOfUsersLabel.setHorizontalAlignment(SwingConstants.TRAILING);
		noOfUsersLabel.setBounds(33, 86, 118, 14);
		frmCouchbaseLitePerf.getContentPane().add(noOfUsersLabel);
		
		usersText = new JTextField();
		usersText.setBounds(161, 83, 59, 20);
		frmCouchbaseLitePerf.getContentPane().add(usersText);
		usersText.setColumns(10);
		
		inEveryLabel = new JLabel("in every");
		inEveryLabel.setHorizontalAlignment(SwingConstants.CENTER);
		inEveryLabel.setBounds(230, 86, 48, 14);
		frmCouchbaseLitePerf.getContentPane().add(inEveryLabel);
		
		rampTimeText = new JTextField();
		rampTimeText.setToolTipText("");
		inEveryLabel.setLabelFor(rampTimeText);
		rampTimeText.setBounds(288, 83, 75, 20);
		frmCouchbaseLitePerf.getContentPane().add(rampTimeText);
		rampTimeText.setColumns(10);
		
		inSecondsLabel = new JLabel("(in seconds)");
		inSecondsLabel.setBounds(373, 86, 81, 14);
		frmCouchbaseLitePerf.getContentPane().add(inSecondsLabel);
		
		sgUrlLabel = new JLabel("Sync Gateway URL");
		sgUrlLabel.setHorizontalAlignment(SwingConstants.RIGHT);
		sgUrlLabel.setBounds(33, 117, 118, 14);
		frmCouchbaseLitePerf.getContentPane().add(sgUrlLabel);
		
		sgUrlText = new JTextField();
		sgUrlLabel.setLabelFor(sgUrlText);
		sgUrlText.setBounds(161, 114, 374, 20);
		frmCouchbaseLitePerf.getContentPane().add(sgUrlText);
		sgUrlText.setColumns(10);
		
		cerfPathLabel = new JLabel("Certificate Path");
		cerfPathLabel.setHorizontalAlignment(SwingConstants.RIGHT);
		cerfPathLabel.setBounds(33, 149, 118, 14);
		frmCouchbaseLitePerf.getContentPane().add(cerfPathLabel);
		
		cerfPath = new JLabel("No file selected");
		cerfPathLabel.setLabelFor(cerfPath);
		cerfPath.setBounds(161, 149, 281, 14);
		frmCouchbaseLitePerf.getContentPane().add(cerfPath);
		
		cerfPathButton = new JButton("Browse");
		cerfPathButton.setBounds(454, 145, 81, 23);
		frmCouchbaseLitePerf.getContentPane().add(cerfPathButton);
		cerfPathButton.addActionListener(this);
		
		userCredLabel = new JLabel("Input Parameters");
		userCredLabel.setHorizontalAlignment(SwingConstants.RIGHT);
		userCredLabel.setBounds(33, 183, 118, 14);
		frmCouchbaseLitePerf.getContentPane().add(userCredLabel);
		
		userCredPath = new JLabel("No file selected");
		userCredLabel.setLabelFor(userCredPath);
		userCredPath.setBounds(161, 183, 281, 14);
		frmCouchbaseLitePerf.getContentPane().add(userCredPath);
		
		userCredButton = new JButton("Browse");
		userCredButton.setBounds(454, 179, 81, 23);
		frmCouchbaseLitePerf.getContentPane().add(userCredButton);
		userCredButton.addActionListener(this);
		
		dbNameLabel = new JLabel("Database Name");
		dbNameLabel.setHorizontalAlignment(SwingConstants.RIGHT);
		dbNameLabel.setBounds(33, 216, 118, 14);
		frmCouchbaseLitePerf.getContentPane().add(dbNameLabel);
		
		dbNamePath = new JLabel("No file selected");
		dbNameLabel.setLabelFor(dbNamePath);
		dbNamePath.setBounds(161, 216, 281, 14);
		frmCouchbaseLitePerf.getContentPane().add(dbNamePath);
		
		dbNameButton = new JButton("Browse");
		dbNameButton.setBounds(454, 212, 81, 23);
		frmCouchbaseLitePerf.getContentPane().add(dbNameButton);
		dbNameButton.addActionListener(this);
		
		statusPanel = new JPanel();
		statusPanel.setBorder(new LineBorder(new Color(0, 0, 0)));
		statusPanel.setBounds(33, 246, 502, 87);
		frmCouchbaseLitePerf.getContentPane().add(statusPanel);
		statusPanel.setLayout(null);
		
		clearButton = new JButton("Clear all");
		clearButton.addActionListener(this);
		clearButton.setBounds(390, 31, 89, 23);
		statusPanel.add(clearButton);
		
		pullButton = new JButton("Pull");
		pullButton.addActionListener(this);
		pullButton.setBounds(27, 11, 89, 23);
		statusPanel.add(pullButton);
		
		stopButton = new JButton("Stop");
		stopButton.setEnabled(false);
		stopButton.addActionListener(this);
		stopButton.setBounds(272, 31, 89, 23);
		statusPanel.add(stopButton);
		
		pushButton = new JButton("Push");
		pushButton.setBounds(144, 11, 89, 23);
		statusPanel.add(pushButton);
		pushButton.addActionListener(this);
		
		pushAndPullButton = new JButton("Push and Pull");
		pushAndPullButton.setBounds(60, 53, 132, 23);
		statusPanel.add(pushAndPullButton);
		pushAndPullButton.addActionListener(this);
		
		statusLabel = new JLabel("Status:");
		statusLabel.setFont(new Font("Tahoma", Font.PLAIN, 13));
		statusLabel.setBounds(33, 344, 505, 14);
		frmCouchbaseLitePerf.getContentPane().add(statusLabel);
		
		progressBar = new JProgressBar();
		progressBar.setStringPainted(true);
		progressBar.setBounds(33, 369, 502, 14);
		frmCouchbaseLitePerf.getContentPane().add(progressBar);
		
		errorLabel = new JLabel("Errors:");
		errorLabel.setHorizontalAlignment(SwingConstants.LEFT);
		errorLabel.setBounds(337, 406, 48, 14);
		frmCouchbaseLitePerf.getContentPane().add(errorLabel);
		
		errorText = new JLabel("");
		errorText.setHorizontalAlignment(SwingConstants.LEFT);
		errorText.setForeground(Color.RED);
		errorText.setBounds(395, 406, 48, 14);
		frmCouchbaseLitePerf.getContentPane().add(errorText);
		
		passedLabel = new JLabel("Passed Transactions:");
		passedLabel.setBounds(65, 406, 127, 14);
		frmCouchbaseLitePerf.getContentPane().add(passedLabel);
		
		passedText = new JLabel("");
		passedText.setForeground(Color.BLACK);
		passedText.setBounds(209, 406, 48, 14);
		frmCouchbaseLitePerf.getContentPane().add(passedText);
		
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		if(e.getSource() == cerfPathButton) {
			JFileChooser fileChooser = new JFileChooser("D:");
			fileChooser.setAcceptAllFileFilterUsed(false);
			FileNameExtensionFilter fileNameRestrict = new FileNameExtensionFilter(".cer files", "cer");
			fileChooser.addChoosableFileFilter(fileNameRestrict);
			int r = fileChooser.showOpenDialog(null);
			if(r == JFileChooser.APPROVE_OPTION) {
				cerfPath.setText(fileChooser.getSelectedFile().getAbsolutePath());
			}else {
				cerfPath.setText("No file selected");
			}
		}
		if(e.getSource() == userCredButton) {
			JFileChooser fileChooser = new JFileChooser("D:");
			fileChooser.setAcceptAllFileFilterUsed(false);
			FileNameExtensionFilter fileNameRestrict = new FileNameExtensionFilter(".txt files", "txt");
			fileChooser.addChoosableFileFilter(fileNameRestrict);
			int r = fileChooser.showOpenDialog(null);
			if(r == JFileChooser.APPROVE_OPTION) {
				userCredPath.setText(fileChooser.getSelectedFile().getAbsolutePath());
			}else {
				userCredPath.setText("No file selected");
			}
		}
		if(e.getSource() == dbNameButton) {
			JFileChooser fileChooser = new JFileChooser("D:");
			fileChooser.setAcceptAllFileFilterUsed(false);
			FileNameExtensionFilter fileNameRestrict = new FileNameExtensionFilter(".txt files", "txt");
			fileChooser.addChoosableFileFilter(fileNameRestrict);
			int r = fileChooser.showOpenDialog(null);
			if(r == JFileChooser.APPROVE_OPTION) {
				dbNamePath.setText(fileChooser.getSelectedFile().getAbsolutePath());
			}else {
				dbNamePath.setText("No file selected");
			}
		}
		if(e.getSource() == pullButton) {
			statusLabel.setText("Status:");
			if(validateInput()) {				
				String temp = statusLabel.getText();
				String outputFilePath = outputPath + "\\" + currentDateAndTime("time") + " OutputLog(Pull).csv";
				String errorFilePath = outputPath + "\\" + currentDateAndTime("time") + " ErrorLog(Pull).csv";
				try {
					FileWriter fw = new FileWriter(outputFilePath, true);
					fw.write("Start Time;Thread;Row Count;User;DB Name;Total Doc;Route ID;Route Count;Time Taken(In Seconds);DB Size(In MB)\n");
					fw.close();
					FileWriter fw1 = new FileWriter(errorFilePath, true);
					fw1.write("Start Time;Thread;User;DB Name;Error Code\n");
					fw1.close();
				} catch(FileNotFoundException fe) {
					System.out.println("Output file not found exception... Please create a path " + getMainPath());
				} catch (IOException e1) {
					System.out.println("FileWriter Exception in Output file...");
				}
				statusLabel.setText(temp += " Started at " + currentDateAndTime("both"));
				progressBar.setValue(1);
				errorText.setText("0");
				passedText.setText("0");
				final long mainInterval = (long)(rampTime*1000);
				worker = new SwingWorker<Void, Void>(){
					@Override
					protected Void doInBackground() throws Exception {
						int temp = 1;
						boolean status = true;
						int lastVal = totalUsers*iterations;
						pullButton.setEnabled(false);
						pushButton.setEnabled(false);
						pushAndPullButton.setEnabled(false);
						stopButton.setEnabled(true);
						clearButton.setEnabled(false);
						while(getRowCount() <= lastVal) {
							try {
								if(((temp-1)%users == 0) && temp!=1) {
									Thread.sleep(mainInterval);
								}
								if(temp <= totalUsers) {
									CbLitePull cblt = new CbLitePull(threadGroup, "Thread-"+Integer.toString(temp), CbLiteGUI.this, Integer.parseInt(pacingText.getText()), iterations, sgUrl, cerf, DB_USER_LIST.get(user_count), DB_PASS_LIST.get(user_count), DB_CHANNEL_LIST.get(user_count), DB_NAME_LIST.get(db_count));
									cblt.setValues(outputFilePath, errorFilePath, lastVal);
									cblt.start();
									temp++;
									changeValues();
								}
							}catch(InterruptedException ie) {
								status = false;
								System.out.println("Stopping all process...");
								break;
							}catch(Exception exception) {
								System.out.println(exception);
							}
						}
						System.out.println("\nPull completed with status: " + status + "\n");
						if(status) {
							progressBar.setValue(100);
						}else {
							progressBar.setValue(0);
						}
						return null;
					}
					@Override
					protected void done() {
						if(threadGroup != null) {
							threadGroup.interrupt();
						}
						if(deleteFolder()) {
							try {
								pullButton.setEnabled(true);
								pushButton.setEnabled(true);
								pushAndPullButton.setEnabled(true);
								stopButton.setEnabled(false);
								clearButton.setEnabled(true);
								String temp = statusLabel.getText();
								statusLabel.setText(temp += " - Ended at " + currentDateAndTime("both"));
							} catch(Exception exception) {
								exception.printStackTrace();
							}
						}else {
							setErrorDialog("Please close the application and try again...", "Error");
						}
					}
				};
				worker.execute();
			}else {
				pullButton.setEnabled(true);
				pushButton.setEnabled(true);
				pushAndPullButton.setEnabled(true);
				stopButton.setEnabled(false);
				clearButton.setEnabled(true);
			}		
		}
		if(e.getSource() == pushButton) {
			statusLabel.setText("Status:");
			if(validateInput()) {				
				String temp = statusLabel.getText();
				String outputFilePath = outputPath + "\\" + currentDateAndTime("time") + " OutputLog(Push).csv";
				String errorFilePath = outputPath + "\\" + currentDateAndTime("time") + " ErrorLog(Push).csv";
				try {
					FileWriter fw = new FileWriter(outputFilePath, true);
					fw.write("Start Time;Thread;Row Count;User;DB Name;Docs Pushed;Time Taken(In Seconds);DB Size(In MB)\n");
					fw.close();
					FileWriter fileWriter = new FileWriter(errorFilePath, true);
					fileWriter.write("Start Time;Thread;User;DB Name;Error Code\n");
					fileWriter.close();
				} catch(FileNotFoundException fe) {
					System.out.println("Output file not found exception... Please create a path " + getMainPath());
				} catch (IOException e1) {
					System.out.println("FileWriter Exception in Output file...");
				}
				statusLabel.setText(temp += " Started at " + currentDateAndTime("both"));
				progressBar.setValue(1);
				errorText.setText("0");
				passedText.setText("0");
				final long mainInterval = (long)(rampTime*1000);
				worker = new SwingWorker<Void, Void>(){
					@Override
					protected Void doInBackground() throws Exception {
						int temp = 1;
						boolean status = true;
						int lastVal = totalUsers*iterations;
						pullButton.setEnabled(false);
						pushButton.setEnabled(false);
						pushAndPullButton.setEnabled(false);
						stopButton.setEnabled(true);
						clearButton.setEnabled(false);
						while(getRowCount() <= lastVal) {
							try {
								if(((temp-1)%users == 0) && temp!=1) {
									Thread.sleep(mainInterval);
								}
								if(temp <= totalUsers) {
									CbLitePush cblt = new CbLitePush(threadGroup, "Thread-"+Integer.toString(temp), CbLiteGUI.this, Integer.parseInt(pacingText.getText()), iterations, sgUrl, cerf, DB_USER_LIST.get(user_count), DB_PASS_LIST.get(user_count), DB_CHANNEL_LIST.get(user_count), DB_NAME_LIST.get(db_count));
									cblt.setValues(outputFilePath, errorFilePath, lastVal);
									cblt.start();
									temp++;
									changeValues();
								}
							}catch(InterruptedException ie) {
								status = false;
								System.out.println("Stopping all process...");
								break;
							}catch(Exception exception) {
								System.out.println(exception);
							}
						}
						System.out.println("\nPush completed with status: " + status + "\n");
						if(status) {
							progressBar.setValue(100);
						}else {
							progressBar.setValue(0);
						}
						return null;
					}
					@Override
					protected void done() {
						if(threadGroup != null) {
							threadGroup.interrupt();
						}
						if(deleteFolder()) {
							try {
								pullButton.setEnabled(true);
								pushButton.setEnabled(true);
								pushAndPullButton.setEnabled(true);
								stopButton.setEnabled(false);
								clearButton.setEnabled(true);
								String temp = statusLabel.getText();
								statusLabel.setText(temp += " - Ended at " + currentDateAndTime("both"));
							} catch(Exception exception) {
								exception.printStackTrace();
							}
						}else {
							setErrorDialog("Please close the application and try again...", "Error");
						}
					}
				};
				worker.execute();
			}else {
				pullButton.setEnabled(true);
				pushButton.setEnabled(true);
				pushAndPullButton.setEnabled(true);
				stopButton.setEnabled(false);
				clearButton.setEnabled(true);
			}		
		}
		if(e.getSource() == pushAndPullButton) {
			statusLabel.setText("Status:");
			if(validateInput()) {				
				String temp = statusLabel.getText();
				String outputFilePath = outputPath + "\\" + currentDateAndTime("time") + " OutputLog(PushAndPull).csv";
				String errorFilePath = outputPath + "\\" + currentDateAndTime("time") + " ErrorLog(PushAndPull).csv";
				try {
					FileWriter fw = new FileWriter(outputFilePath, true);
					fw.write("Start Time;Thread;Row Count;User;DB Name;Total Doc;Route ID;Route Count;Docs Pushed;Time Taken(In Seconds);DB Size(In MB)\n");
					fw.close();
					FileWriter fileWriter = new FileWriter(errorFilePath, true);
					fileWriter.write("Start Time;Thread;User;DB Name;Error Code\n");
					fileWriter.close();
				} catch(FileNotFoundException fe) {
					System.out.println("Output file not found exception... Please create a path " + getMainPath());
				} catch (IOException e1) {
					System.out.println("FileWriter Exception in Output file...");
				}
				statusLabel.setText(temp += " Started at " + currentDateAndTime("both"));
				progressBar.setValue(1);
				errorText.setText("0");
				passedText.setText("0");
				final long mainInterval = (long)(rampTime*1000);
				worker = new SwingWorker<Void, Void>(){
					@Override
					protected Void doInBackground() throws Exception {
						int temp = 1;
						boolean status = true;
						int lastVal = totalUsers*iterations;
						pullButton.setEnabled(false);
						pushButton.setEnabled(false);
						pushAndPullButton.setEnabled(false);
						stopButton.setEnabled(true);
						clearButton.setEnabled(false);
						while(getRowCount() <= lastVal) {
							try {
								if(((temp-1)%users == 0) && temp!=1) {
									Thread.sleep(mainInterval);
								}
								if(temp <= totalUsers) {
									CbLitePushAndPull cblt = new CbLitePushAndPull(threadGroup, "Thread-"+Integer.toString(temp), CbLiteGUI.this, Integer.parseInt(pacingText.getText()), iterations, sgUrl, cerf, DB_USER_LIST.get(user_count), DB_PASS_LIST.get(user_count), DB_CHANNEL_LIST.get(user_count), DB_NAME_LIST.get(db_count));
									cblt.setValues(outputFilePath, errorFilePath, lastVal);
									cblt.start();
									temp++;
									changeValues();
								}
							}catch(InterruptedException ie) {
								status = false;
								System.out.println("Stopping all process...");
								break;
							}catch(Exception exception) {
								System.out.println(exception);
							}
						}
						System.out.println("\nPush and Pull completed with status: " + status + "\n");
						if(status) {
							progressBar.setValue(100);
						}else {
							progressBar.setValue(0);
						}
						return null;
					}
					@Override
					protected void done() {
						if(threadGroup != null) {
							threadGroup.interrupt();
						}
						if(deleteFolder()) {
							try {
								pullButton.setEnabled(true);
								pushButton.setEnabled(true);
								pushAndPullButton.setEnabled(true);
								stopButton.setEnabled(false);
								clearButton.setEnabled(true);
								String temp = statusLabel.getText();
								statusLabel.setText(temp += " - Ended at " + currentDateAndTime("both"));
							} catch(Exception exception) {
								exception.printStackTrace();
							}
						}else {
							setErrorDialog("Please close the application and try again...", "Error");
						}
					}
				};
				worker.execute();
			}else {
				pullButton.setEnabled(true);
				pushButton.setEnabled(true);
				pushAndPullButton.setEnabled(true);
				stopButton.setEnabled(false);
				clearButton.setEnabled(true);
			}		
		}
		if(e.getSource() == stopButton) {
			worker.cancel(true);
		}
		if(e.getSource() == clearButton) {
			resetUI();
			initiateValues();
		}
	}

	private boolean validateInput() {
		boolean bool = true;
		String msg = "";
		if(!totalUsersText.getText().equals("")) {
			try {
				totalUsers = Integer.parseInt(totalUsersText.getText());
			}catch(NumberFormatException exception) {
				inputError += "Total no of users, ";
			}
		}else {
			blankError += "Total no of users, ";
		}
		if(!usersText.getText().equals("")) {
			try {
				users = Integer.parseInt(usersText.getText());
				if(users > totalUsers) {
					users = totalUsers;
				}
			}catch(NumberFormatException exception) {
				inputError += "Number of Users, ";
			}
		}else {
			blankError += "Number of Users, ";
		}
		if(!rampTimeText.getText().equals("")) {
			try {
				rampTime = Double.parseDouble(rampTimeText.getText());
			}catch(NumberFormatException exception) {
				inputError += "Ramp up time, ";
			}
		}else {
			blankError += "Ramp up time, ";
		}
		if(!iterationsText.getText().equals("")) {
			try {
				iterations = Integer.parseInt(iterationsText.getText());
			}catch(NumberFormatException exception) {
				inputError += "Iterations, ";
			}
		}else {
			blankError += "Iterations, ";
		}
		if(!steadyStateText.getText().equals("")) {
			try {
				steadyState = Integer.parseInt(steadyStateText.getText());
			}catch(NumberFormatException exception) {
				inputError += "Steady State";
			}
		}else {
			blankError += "Steady State, ";
		}	
		if(sgUrlText.getText().equals("")) {
			blankError += "SyncGateway URL, ";
		}else {
			sgUrl = sgUrlText.getText();
			try {
				URLEndpoint uRLEndpoint = new URLEndpoint(new URI(sgUrl));
			} catch(Exception e) {
				inputError += "SyncGateway URL, ";
			}
		}
		if(cerfPath.getText().equals("No file selected")) {
			blankError += "Cerficate Path, ";
		}else {
			cerf = cerfPath.getText();
		}
		if(userCredPath.getText().equals("No file selected")) {
			blankError += "Username and Password, ";
		}else {
			userCred = userCredPath.getText();
		}
		if(dbNamePath.getText().equals("No file selected")) {
			blankError += "Database Path. ";
		}else {
			dbName = dbNamePath.getText();
		}
		if(!inputError.equals("Incorrect Input - ")) {
			msg += inputError;
			bool = false;
		}
		if(!blankError.equals("Field empty - ")) {
			if(msg.equals("")) {
				msg += blankError;
			}else {
				msg += "\n" + blankError;
			}
			bool = false;
		}
		if(!msg.equals("")) {
			setDialog(msg, "Please check the input fields");
		}
		initiateValues();
		if(bool) {
			msg = "";
			if(iterations <= 0) {
				iterations = 1;
			}
			try {
				FileReader file_input_users = new FileReader(userCred);
				Scanner scanner = new Scanner(file_input_users);
			    while (scanner.hasNext()){
			        String line = scanner.nextLine();
			        String[] lines = line.split(",");
			    	DB_USER_LIST.add(lines[0]);
			    	DB_PASS_LIST.add(lines[1]);
			    	DB_CHANNEL_LIST.add(lines[2]);
			    }
			    scanner.close();
			    total_users = DB_USER_LIST.size();
			    if(total_users == 0) {
			    	throw new Exception();
			    }
			} catch (FileNotFoundException e) {
				setDialog("Username, Password, RouteID File not found..", "Please check the input fields");
				bool = false;
			} catch(Exception e){
				if(total_users == 0) {
					setDialog("Username, Password, RouteID File empty...", "Please check the input fields");
				}else {
					setDialog("Error in Username, Password, RouteID File. Please check the file content format...", "Please check the input fields");
				}
				bool = false;
			}
			try {
				FileReader file_input_db_name = new FileReader(dbName);
			    Scanner scanner = new Scanner(file_input_db_name);
			    while (scanner.hasNext()){
			        String line = scanner.nextLine();
			        DB_NAME_LIST.add(line);
			    }
			    scanner.close();
			    total_db = DB_NAME_LIST.size();
			    if(total_db == 0) {
			    	throw new Exception();
			    }
			} catch (FileNotFoundException e) {
				setDialog("Database File not found..", "Please check the input fields");
				bool = false;
			}catch(Exception e){
				if(total_db == 0) {
					setDialog("Database File is empty...", "Please check the input fields");
				}else {
					setDialog("Error in Database File. Please check the file content...", "Please check the input fields");
				}
				bool = false;
			}
			if(!deleteFolder()) {
				setDialog("Please close the application and try again...", "Error");
			}
		}
		return bool;
	}
	
	public synchronized void setBar(int val) {
		if(progressBar.getValue() < val && val!=100) {
			progressBar.setValue(val);
		}
	}
	
	public synchronized void changeValues() {
		if(user_count == total_users-1) {
			user_count = 0;
			if(db_count == total_db-1) {
				db_count = 0;
			}else {
				db_count++;
			}
		}else {
			user_count++;
		}
	}
	
	public static String currentDateAndTime(String value) {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter;
		switch(value) {
			case "date":{
				formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
				break;
			}
			case "time":{
				formatter = DateTimeFormatter.ofPattern("hh-mm-ss a");
				break;
			}
			case "both":{
				formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a");
				break;
			}
			default:{
				formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
				break;
			}
		}
		return now.format(formatter);        
	}
	
	private void initiateValues() {
		DB_USER_LIST.clear();
		DB_PASS_LIST.clear();
		DB_NAME_LIST.clear();
		DB_CHANNEL_LIST.clear();
		rowCount = 1;
		user_count = 0;
		db_count = 0;
		inputError = "Incorrect Input - ";
		blankError = "Field empty - ";
		errorCount = 0;
		passedCount = 0;
	}
	
	private void resetUI() {
		totalUsersText.setText("");
		usersText.setText("");
		rampTimeText.setText("");
		iterationsText.setText("");
		steadyStateText.setText("");
		sgUrlText.setText("");
		cerfPath.setText("No file selected");
		userCredPath.setText("No file selected");
		dbNamePath.setText("No file selected");
		pullButton.setEnabled(true);
		pushButton.setEnabled(true);
		pushAndPullButton.setEnabled(true);
		stopButton.setEnabled(false);
		clearButton.setEnabled(true);
		statusLabel.setText("Status:");
		pacingText.setText("0");
		progressBar.setValue(0);
		errorText.setText("");
		passedText.setText("");
	}
	
	private void setPacing() {
		int ss = 0;
		int it = 0;
		int pac = 0;
		if(!iterationsText.getText().equals("")) {
			try {
				it = Integer.parseInt(iterationsText.getText());
			}catch(NumberFormatException exception) {}
		}
		if(!steadyStateText.getText().equals("")) {
			try {
				ss = Integer.parseInt(steadyStateText.getText());
			}catch(NumberFormatException exception) {}
		}
		try {
			pac = ((ss*60)/it);
		}catch(ArithmeticException e) {
			pac = 0;
		}
		pacingText.setText(String.valueOf(pac));
	}
	
	public void setDialog(String msg, String title) {
		JOptionPane.showMessageDialog(this.frmCouchbaseLitePerf, msg, title, JOptionPane.PLAIN_MESSAGE);
	}
	
	public void setErrorDialog(String msg, String title) {
		try {
			int res = JOptionPane.showConfirmDialog(this.frmCouchbaseLitePerf, msg, title, JOptionPane.DEFAULT_OPTION);
			if(res == 0) {
				System.exit(0);
			}
		}catch(Exception exception) {
			exception.printStackTrace();
		}
	}
	
	private boolean deleteFolder() {
		try {
			if(progressBar.getValue() == 100) {
				while(!FileUtils.deleteRecursive(getMainPath() + "\\Resources\\")) {
					Thread.sleep(1000L);
				}
			}else {
				for(int temp=0;temp<5;temp++) {
					if(FileUtils.deleteRecursive(getMainPath() + "\\Resources\\")) {
						break;
					}else {
						Thread.sleep(6000L);
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
		if(FileUtils.deleteRecursive(getMainPath() + "\\Resources\\")) {
			return true;
		}
		return false;
	}
	
}