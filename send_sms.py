from twilio.rest import Client

# Your Account SID from twilio.com/console
account_sid = "account_sid"
# Your Auth Token from twilio.com/console
auth_token  = "auth_token"

client = Client(account_sid, auth_token)

message = client.messages.create(
    to="+525555555555", 
    from_="+1555555556",
    body="¿Qué animal come con la cola. v2?")

print(message.sid)



### Prdo
#ec2-34-201-118-70.compute-1.amazonaws.com
#ec2-34-201-118-70.compute-1.amazonaws.com/flaskapp
#ec2-34-201-118-70.compute-1.amazonaws.com/sms
#http://ec2-34-201-118-70.compute-1.amazonaws.com/sms

### Dev
#ec2-54-209-126-166.compute-1.amazonaws.com/sms

#whatsapp://send?phone=+14155238886&text=join left-foreign
