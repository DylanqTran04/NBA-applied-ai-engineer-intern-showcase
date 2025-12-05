import { Component, ViewChild, ElementRef, AfterViewChecked } from '@angular/core';
import { ChatService } from './services/chat.service';

interface Evidence {
  table: string;
  id: number;
  details?: string;
  date?: string;
}

interface Message {
  sender: 'user' | 'bot';
  text: string;
  evidence?: Evidence[];
}

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements AfterViewChecked {
  @ViewChild('messagesContainer') private messagesContainer!: ElementRef;

  title = 'NBA Stats Assistant';
  messages: Message[] = [];
  userInput = '';
  isLoading = false;

  suggestions = [
    'Who won Christmas Day 2023?',
    'How many points did Luka score?',
    'Victor Wembanyama debut stats'
  ];

  private shouldScrollToBottom = false;

  constructor(private chatService: ChatService) {
    // Add welcome message
    this.messages.push({
      sender: 'bot',
      text: 'Welcome to the NBA Stats Assistant! Ask me about NBA games from the 2023-24 and 2024-25 seasons.',
      evidence: []
    });
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  sendMessage(): void {
    const input = this.userInput.trim();
    if (!input || this.isLoading) {
      return;
    }

    // Add user message
    this.messages.push({ sender: 'user', text: input });
    this.userInput = '';
    this.isLoading = true;
    this.shouldScrollToBottom = true;

    // Call the chat service
    this.chatService.sendMessage(input).subscribe({
      next: (res: any) => {
        const reply = res?.answer ?? 'No answer provided.';
        const evidence = res?.evidence ?? [];

        this.messages.push({
          sender: 'bot',
          text: reply,
          evidence: evidence
        });

        this.isLoading = false;
        this.shouldScrollToBottom = true;
      },
      error: (err) => {
        console.error('Error calling chat API:', err);
        this.messages.push({
          sender: 'bot',
          text: '❌ Error contacting the server. Please make sure the backend is running on port 8000.',
          evidence: []
        });
        this.isLoading = false;
        this.shouldScrollToBottom = true;
      }
    });
  }

  useSuggestion(suggestion: string): void {
    if (!this.isLoading) {
      this.userInput = suggestion;
      this.sendMessage();
    }
  }

  private scrollToBottom(): void {
    try {
      if (this.messagesContainer) {
        const element = this.messagesContainer.nativeElement;
        element.scrollTop = element.scrollHeight;
      }
    } catch (err) {
      console.error('Error scrolling to bottom:', err);
    }
  }
}
