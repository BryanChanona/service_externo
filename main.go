package main

import (
	"consumer/models"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"net/http"
	"bytes"

	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	// Cargar variables de entorno
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error al cargar el archivo .env: %v", err)
	}

	// Obtener credenciales desde las variables de entorno
	user := os.Getenv("RABBITMQ_USER")
	password := os.Getenv("RABBITMQ_PASSWORD")
	host := os.Getenv("RABBITMQ_IP")
	port := os.Getenv("RABBITMQ_PORT")

	// Construir la URL de conexión
	rabbitURL := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port)

	// Conectar con RabbitMQ
	conn, err := amqp091.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("❌ Error al conectar con RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Abrir un canal
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Error al abrir un canal: %v", err)
	}
	defer ch.Close()

	// Declarar el exchange (fanout)
	err = ch.ExchangeDeclare(
		"logs",   // Nombre del exchange
		"fanout", // Tipo de exchange
		true,     // Durable
		false,    // Auto-deleted
		false,    // Internal
		false,    // No-wait
		nil,      // Arguments
	)
	if err != nil {
		log.Fatalf(" Error al declarar el exchange: %v", err)
	}

	// Declarar la cola
	q, err := ch.QueueDeclare(
		"books_queue", // Nombre de la cola
		true,          // Durable
		false,         // Auto-Delete
		false,         // Exclusive
		false,         // No-wait
		nil,           // Arguments
	)
	if err != nil {
		log.Fatalf(" Error al declarar la cola: %v", err)
	}

	// Vincular la cola al exchange
	err = ch.QueueBind(
		q.Name, // Nombre de la cola
		"",     // Routing key (vacío porque es fanout)
		"logs", // Exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf(" Error al vincular la cola al exchange: %v", err)
	}

	// Consumir mensajes de la cola
	msgs, err := ch.Consume(
		q.Name, // Nombre de la cola
		"",     // Consumer
		true,   // Auto-Ack (reconoce automáticamente los mensajes)
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Arguments
	)
	if err != nil {
		log.Fatalf(" Error al consumir mensajes: %v", err)
	}

	log.Println(" Escuchando mensajes de RabbitMQ...")

	// Canal para mantener el consumidor en ejecución
	forever := make(chan bool)

	// Procesar mensajes en un goroutine
	go func() {
		for msg := range msgs {
			var book models.Book
			err := json.Unmarshal(msg.Body, &book)
			if err != nil {
				log.Println("Error decoding JSON:", err)
				continue
			}

			// Enviar a API Loans
			sendToAPILoans(book)
		}
	}()

	<-forever // Mantiene el consumidor en ejecución
}

func sendToAPILoans(book models.Book) {
	jsonData, _ := json.Marshal(book)
	resp, err := http.Post("http://localhost:8081/books", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Println("Error sending to API Loans:", err)
		return
	}
	defer resp.Body.Close()

	log.Println("Sent to API Loans:", resp.Status)
}