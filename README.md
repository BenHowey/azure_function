Azure function to reformat csv to json in accordance with ERR template.

Example command:
`curl -F "file=@Expenses_flow_test.csv" https://my-test-func001.azurewebsites.net/api/file` or for local testing `curl -F "file=@Expenses_flow_test.csv" http://localhost/api/file`

To debug the function locally, put a breakpoint in the code. Then run the debugger which runs a webserver on localhost.  Hit the end point from a terminal with a request similar to above (Look in the debug output for the port numbers - usually 9091).  You will see the code hit the breakpoint and then you can debug as normal.  Running this through the debugger (which is running the webserver in the background) runs ngrok in the background to handle the forwarding to https rather than http.

