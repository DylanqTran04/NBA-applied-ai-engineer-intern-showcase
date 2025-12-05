import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

import { BaseService } from './base.service';

@Injectable({
  providedIn: 'root'
})
export class ChatService extends BaseService {
  constructor(protected override http: HttpClient) {
    super(http);
  }

  sendMessage(question: string): Observable<any> {
    const endpoint = `${this.baseUrl}/chat`;
    return this.post(endpoint, { question });
  }
}