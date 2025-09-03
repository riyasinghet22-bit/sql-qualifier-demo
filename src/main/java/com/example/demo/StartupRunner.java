package com.example.demo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Profile("!test")
@Component
public class StartupRunner implements ApplicationRunner {

  private final WebClient http = WebClient.builder()
      .exchangeStrategies(ExchangeStrategies.builder()
          .codecs(c -> c.defaultCodecs().maxInMemorySize(2 * 1024 * 1024))
          .build())
      .baseUrl("https://bfhldevapigw.healthrx.co.in")
      .build();

  private static final String NAME   = "<YOUR NAME>";
  private static final String REG_NO = "RVCE23CS038";
  private static final String EMAIL  = "<YOUR EMAIL>";

  @Override
  public void run(ApplicationArguments args) {
    try {
      System.out.println("➡ Generating webhook (with retries)...");
      GenerateResp resp = http.post()
          .uri("/hiring/generateWebhook/JAVA")
          .contentType(MediaType.APPLICATION_JSON)
          .bodyValue(new GenerateReq(NAME, REG_NO, EMAIL))
          .retrieve()
          .onStatus(HttpStatusCode::isError, this::readErrorBody)
          .bodyToMono(GenerateResp.class)
          .timeout(Duration.ofSeconds(15))
          .retryWhen(
        Retry.backoff(5, Duration.ofSeconds(1))
         .maxBackoff(Duration.ofSeconds(10))
         .filter(t -> t.toString().contains("503") || t.toString().contains("5xx"))
)
          .block();

      if (resp == null || resp.webhook == null || resp.accessToken == null) {
        throw new IllegalStateException("Missing webhook/accessToken in response");
      }

      System.out.println("✅ Webhook: " + resp.webhook);
      System.out.println("✅ Access token received.");

      int lastTwo = extractLastTwoDigits(REG_NO);
      boolean isOdd = (lastTwo % 2 == 1);
      System.out.println("ℹ Last two digits = " + lastTwo + " → " + (isOdd ? "Q1" : "Q2"));

      String q2 =
          "SELECT e.EMP_ID, e.FIRST_NAME, e.LAST_NAME, d.DEPARTMENT_NAME, " +
          "COUNT(e2.EMP_ID) AS YOUNGER_EMPLOYEES_COUNT " +
          "FROM EMPLOYEE e " +
          "JOIN DEPARTMENT d ON e.DEPARTMENT = d.DEPARTMENT_ID " +
          "LEFT JOIN EMPLOYEE e2 ON e.DEPARTMENT = e2.DEPARTMENT " +
          "AND e2.DOB > e.DOB " +
          "GROUP BY e.EMP_ID, e.FIRST_NAME, e.LAST_NAME, d.DEPARTMENT_NAME " +
          "ORDER BY e.EMP_ID DESC;";

      String finalSql = isOdd ? "SELECT 1;" : q2;

      System.out.println("➡ Submitting finalQuery…");
      String submitResult = http.post()
          .uri(resp.webhook)
          .header(HttpHeaders.AUTHORIZATION, resp.accessToken)
          .contentType(MediaType.APPLICATION_JSON)
          .bodyValue(new SubmitReq(finalSql))
          .retrieve()
          .onStatus(HttpStatusCode::isError, this::readErrorBody)
          .bodyToMono(String.class)
          .timeout(Duration.ofSeconds(15))
          .block();

      System.out.println("✅ Submission response: " + submitResult);
    } catch (Exception ex) {
      System.err.println("💥 Fatal: " + ex.getClass().getSimpleName() + " - " + ex.getMessage());
      ex.printStackTrace(System.err);
    }
  }

  private Mono<? extends Throwable> readErrorBody(ClientResponse resp) {
    return resp.body(BodyExtractors.toDataBuffers())
        .reduce((a, b) -> { a.write(b); return a; })
        .map(buf -> {
          byte[] bytes = new byte[buf.readableByteCount()];
          buf.read(bytes);
          String body = new String(bytes, StandardCharsets.UTF_8);
          return new RuntimeException("HTTP " + resp.statusCode() + " – " + body);
        })
        .switchIfEmpty(Mono.just(new RuntimeException("HTTP " + resp.statusCode() + " with empty body")));
  }

  private int extractLastTwoDigits(String reg) {
    String digits = reg.replaceAll("\\D+", "");
    if (digits.isEmpty()) throw new IllegalArgumentException("regNo has no digits");
    if (digits.length() == 1) return Integer.parseInt(digits);
    return Integer.parseInt(digits.substring(digits.length() - 2));
  }

  static class GenerateReq {
    public String name;
    @JsonProperty("regNo")
    public String regNo;
    public String email;
    public GenerateReq(String name, String regNo, String email) {
      this.name = name; this.regNo = regNo; this.email = email;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class GenerateResp {
    public String webhook;
    public String accessToken;
  }

  static class SubmitReq {
    @JsonProperty("finalQuery")
    public String finalQuery;
    public SubmitReq(String finalQuery) { this.finalQuery = finalQuery; }
  }
}