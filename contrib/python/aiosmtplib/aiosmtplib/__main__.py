from aiosmtplib.smtp import SMTP, SMTP_PORT


raw_hostname = input("SMTP server hostname [localhost]: ")  # nosec
raw_port = input(f"SMTP server port [{SMTP_PORT}]: ")  # nosec
raw_sender = input("From: ")  # nosec
raw_recipients = input("To: ")  # nosec

hostname = raw_hostname or "localhost"
port = int(raw_port) if raw_port else SMTP_PORT
recipients = raw_recipients.split(",")
lines: list[str] = []

print("Enter message, end with ^D:")
while True:
    try:
        lines.append(input())  # nosec
    except EOFError:
        break

message = "\n".join(lines)
message_len = len(message.encode("utf-8"))
print(f"Message length (bytes): {message_len}")

smtp_client = SMTP(hostname=hostname or "localhost", port=port, start_tls=False)
sendmail_errors, sendmail_response = smtp_client.sendmail_sync(
    raw_sender, recipients, message
)

print(f"Server response: {sendmail_response}")
