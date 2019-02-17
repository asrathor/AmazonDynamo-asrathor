package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
		Button lDump = (Button) findViewById(R.id.button1);
		Button gDump = (Button) findViewById(R.id.button2);
		lDump.setOnClickListener(new Button.OnClickListener() {
			@Override
			public void onClick(View v) {
                /*
                 * https://developer.android.com/reference/android/database/Cursor.html
                 * Taken from PA3.
                 */
				Cursor cursor = getContentResolver().query(mUri,null,"@",null,null);
				if(cursor == null){
					//Log.v(TAG,"Cursor is null");
				}
				if(cursor.moveToFirst()){
					do{
						String key = cursor.getString(cursor.getColumnIndex("key"));
						String value = cursor.getString(cursor.getColumnIndex("value"));
						String message = "Key: " + key + "Value: " + value;
						tv.append(message);
						tv.append("\n");
					} while(cursor.moveToNext());
				}
			}
		});
		gDump.setOnClickListener(new Button.OnClickListener() {
			@Override
			public void onClick(View v) {
				Cursor cursor = getContentResolver().query(mUri,null,"*",null,null);
				if(cursor == null){
					//Log.v(TAG,"Cursor is null");
				}
				if(cursor.moveToFirst()){
					do{
						String key = cursor.getString(cursor.getColumnIndex("key"));
						String value = cursor.getString(cursor.getColumnIndex("value"));
						String message = "Key: " + key + "Value: " + value;
						tv.append(message);
						tv.append("\n");
					} while(cursor.moveToNext());
				}
			}
		});
	}

	/*
	 * To build a URI for content provider. Referred from PA3.
     */
	private Uri buildUri(String content, String s) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(s);
		uriBuilder.scheme(content);
		return uriBuilder.build();
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
