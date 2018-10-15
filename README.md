# sesam-dedupio
Dedupe.io powered service for finding duplicates in Sesam.io powered applications


# Usage

```
{
  "_id": "dedupe-service-name",
  "type": "system:microservice",
  "docker": {
    "environment": {
      "INSTANCE": "$ENV(sesam-node)",
      "JWT": "$SECRET(JWT)",
      "KEYS": "Email, FirstName, LastName, Phone, MailingCity, MailingCountry, MailingState, MailingStreet, MobilePhone, Name, Title",
      "SETTINGS_FILE": "url or path to settings file",
      "SOURCE": "published-source",
      "TARGET": "target-receiver-pipe"
    },
    "image": "ohuenno/dedupe",
    "port": 5000
  }
}
```

