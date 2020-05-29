package lib
import(

	"log"
	"contrib.go.opencensus.io/exporter/zipkin"
	"go.opencensus.io/trace"

	openzipkin "github.com/openzipkin/zipkin-go"
	zipkinHTTP "github.com/openzipkin/zipkin-go/reporter/http"
)
func RegisterZipkin(){
	localEndpoint, err := openzipkin.NewEndpoint("golangsvc", "192.168.1.61:8080") 
	if err != nil { 
			log.Fatalf("Failed to create Zipkin exporter: %v", err)
	} 
	reporter := zipkinHTTP.NewReporter("http://localhost:9411/api/v2/spans") 
	exporter := zipkin.NewExporter(reporter, localEndpoint) 
	trace.RegisterExporter(exporter) 
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
}