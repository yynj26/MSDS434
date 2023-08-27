import http.server
import socketserver

PORT = 8002

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("ec2-34-102-30-180.us-west-2.compute.amazonaws.com", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
